package verifier

import (
	"context"

	"github.com/10gen/migration-verifier/internal/types"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	recheckQueue   = "recheckQueue"
	maxBSONObjSize = 16 * 1024 * 1024
)

// RecheckPrimaryKey stores the implicit type of recheck to perform
// Currently, we only handle document mismatches/change stream updates,
// so DatabaseName, CollectionName, and DocumentID must always be specified.
type RecheckPrimaryKey struct {
	Generation     int         `bson:"generation"`
	DatabaseName   string      `bson:"db"`
	CollectionName string      `bson:"coll"`
	DocumentID     interface{} `bson:"docID"`
}

// RecheckDoc stores the necessary information to know which documents must be rechecked.
type RecheckDoc struct {
	PrimaryKey RecheckPrimaryKey `bson:"_id"`
	DataSize   int               `bson:"dataSize"`
}

type recheckSpec struct {
	dbName   string
	collName string
	docID    any
	dataSize int
}

// InsertFailedCompareRecheckDocs is for inserting RecheckDocs based on failures during Check.
func (verifier *Verifier) InsertFailedCompareRecheckDocs(
	namespace string, documentIDs []interface{}, dataSizes []int) error {
	dbName, collName := SplitNamespace(namespace)

	recheckSpecs := lo.Map(
		documentIDs,
		func(id any, i int) recheckSpec {
			return recheckSpec{
				dbName:   dbName,
				collName: collName,
				docID:    id,
				dataSize: dataSizes[i],
			}
		},
	)

	return verifier.insertRecheckDocs(
		context.Background(),
		recheckSpecs,
	)
}

func (verifier *Verifier) InsertChangeEventRecheckDocs(ctx context.Context, changeEvents []ParsedEvent) error {
	recheckSpecs := lo.Map(
		changeEvents,
		func(event ParsedEvent, i int) recheckSpec {
			return recheckSpec{
				dbName:   event.Ns.DB,
				collName: event.Ns.Coll,
				docID:    event.DocKey.ID,

				// We don't know the document sizes for documents for all change events,
				// so just be conservative and assume they are maximum size.
				//
				// Note that this prevents us from being able to report a meaningful
				// total data size for noninitial generations in the log.
				dataSize: maxBSONObjSize,
			}
		},
	)

	return verifier.insertRecheckDocs(ctx, recheckSpecs)
}

func (verifier *Verifier) insertRecheckDocs(
	ctx context.Context,
	recheckSpecs []recheckSpec,
) error {
	verifier.mux.Lock()
	defer verifier.mux.Unlock()

	generation, _ := verifier.getGenerationWhileLocked()

	// NB: This shouldn’t exceed 16 MiB since it will have come from a change
	// event batch that would have fit into that limit.
	updates := lo.Map(
		recheckSpecs,
		func(rSpec recheckSpec, _ int) bson.M {
			pk := RecheckPrimaryKey{
				Generation:     generation,
				DatabaseName:   rSpec.dbName,
				CollectionName: rSpec.collName,
				DocumentID:     rSpec.docID,
			}

			recheckDoc := RecheckDoc{
				PrimaryKey: pk,
				DataSize:   rSpec.dataSize,
			}

			return bson.M{
				"q":      bson.M{"_id": pk},
				"u":      recheckDoc,
				"upsert": true,
			}
		},
	)

	// A single update command should outperform separate
	// updates for each recheck--even if done with BulkWrite.
	// Unfortunately, the driver doesn’t expose a bulk-replace
	// command, so we have to do it via RunCommand.
	err := verifier.verificationDatabase().RunCommand(
		ctx,
		bson.D{
			{"update", recheckQueue},
			{"updates", updates},
		},
	).Err()

	if err == nil {
		verifier.logger.Debug().Msgf("Persisted %d recheck doc(s) for generation %d", len(recheckSpecs), generation)
	}

	return errors.Wrapf(err, "failed to persist %d rechecks", len(recheckSpecs))
}

// ClearRecheckDocs deletes the previous generation’s recheck
// documents from the verifier’s metadata.
//
// The verifier **MUST** be locked when this function is called (or panic).
func (verifier *Verifier) ClearRecheckDocs(ctx context.Context) error {
	prevGeneration := verifier.getPreviousGenerationWhileLocked()

	verifier.logger.Debug().Msgf("Deleting generation %d’s %s documents", prevGeneration, recheckQueue)

	_, err := verifier.verificationDatabase().Collection(recheckQueue).DeleteMany(
		ctx, bson.D{{"_id.generation", prevGeneration}})
	return err
}

func (verifier *Verifier) getPreviousGenerationWhileLocked() int {
	generation, _ := verifier.getGenerationWhileLocked()
	if generation < 1 {
		panic("This function is forbidden before generation 1!")
	}

	return generation - 1
}

// GenerateRecheckTasks fetches the previous generation’s recheck
// documents from the verifier’s metadata and creates current-generation
// document-verification tasks from them.
//
// The verifier **MUST** be locked when this function is called (or panic).
func (verifier *Verifier) GenerateRecheckTasks(ctx context.Context) error {
	prevGeneration := verifier.getPreviousGenerationWhileLocked()

	verifier.logger.Debug().Msgf("Creating recheck tasks from generation %d’s %s documents", prevGeneration, recheckQueue)

	// We generate one recheck task per collection, unless
	// 1) The size of the list of IDs would exceed 12MB (a very conservative way of avoiding
	//    the 16MB BSON limit)
	// 2) The size of the data would exceed our desired partition size.  This limits memory use
	//    during the recheck phase.
	prevDBName, prevCollName := "", ""
	var idAccum []interface{}
	var idLenAccum int
	var dataSizeAccum int64
	const maxIdsSize = 12 * 1024 * 1024
	cursor, err := verifier.verificationDatabase().Collection(recheckQueue).Find(
		ctx, bson.D{{"_id.generation", prevGeneration}}, options.Find().SetSort(bson.D{{"_id", 1}}))
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)
	// We group these here using a sort rather than using aggregate because aggregate is
	// subject to a 16MB limit on group size.
	for cursor.Next(ctx) {
		err := cursor.Err()
		if err != nil {
			return err
		}
		var doc RecheckDoc
		err = cursor.Decode(&doc)
		if err != nil {
			return err
		}
		idRaw := cursor.Current.Lookup("_id", "docID")
		idLen := len(idRaw.Value)

		verifier.logger.Debug().Msgf("Found persisted recheck doc for %s.%s", doc.PrimaryKey.DatabaseName, doc.PrimaryKey.CollectionName)

		if doc.PrimaryKey.DatabaseName != prevDBName ||
			doc.PrimaryKey.CollectionName != prevCollName ||
			idLenAccum >= maxIdsSize ||
			dataSizeAccum >= verifier.partitionSizeInBytes {
			namespace := prevDBName + "." + prevCollName
			if len(idAccum) > 0 {
				err := verifier.InsertFailedIdsVerificationTask(idAccum, types.ByteCount(dataSizeAccum), namespace)
				if err != nil {
					return err
				}
				verifier.logger.Debug().Msgf(
					"Created ID verification task for namespace %s with %d ids, "+
						"%d id bytes and %d data bytes",
					namespace, len(idAccum), idLenAccum, dataSizeAccum)
			}
			prevDBName = doc.PrimaryKey.DatabaseName
			prevCollName = doc.PrimaryKey.CollectionName
			idLenAccum = 0
			dataSizeAccum = 0
			idAccum = []interface{}{}
		}
		idLenAccum += idLen
		dataSizeAccum += int64(doc.DataSize)
		idAccum = append(idAccum, doc.PrimaryKey.DocumentID)
	}
	if len(idAccum) > 0 {
		namespace := prevDBName + "." + prevCollName
		err := verifier.InsertFailedIdsVerificationTask(idAccum, types.ByteCount(dataSizeAccum), namespace)
		if err != nil {
			return err
		}
		verifier.logger.Debug().Msgf(
			"Created ID verification task for namespace %s with %d ids, "+
				"%d id bytes and %d data bytes",
			namespace, len(idAccum), idLenAccum, dataSizeAccum)
	}
	return nil
}
