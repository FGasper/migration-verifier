package verifier

import (
	"context"

	"github.com/10gen/migration-verifier/internal/util"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const (
	srcResumeTokenDocID = "srcResumeToken"
	dstResumeTokenDocID = "dstResumeToken"
)

type recheckPersistorMessage interface {
	isRecheckMessage()
}

type sourceResumeToken bson.Raw

func (sourceResumeToken) isRecheckMessage() {}

type destinationResumeToken bson.Raw

func (destinationResumeToken) isRecheckMessage() {}

type recheckBatch []ParsedEvent

func (recheckBatch) isRecheckMessage() {}

func (v *Verifier) runPersistor(
	ctx context.Context,
	metaColl *mongo.Collection,
) (chan<- recheckPersistorMessage, *util.Eventual[error]) {
	inChan := make(chan recheckPersistorMessage, 1_000)

	errEventual := util.NewEventual[error]()

	go func() {
		var srcToken, dstToken bson.Raw
		var allEvents []ParsedEvent

		for {
			select {
			case <-ctx.Done():
				errEventual.Set(nil)
				return
			case msg, more := <-inChan:
				if !more {
					v.logger.Debug().
						Msg("Persistor thread input channel closed. Persistor exiting.")

					return
				}

				switch typedMsg := msg.(type) {
				case sourceResumeToken:
					srcToken = bson.Raw(typedMsg)
				case destinationResumeToken:
					dstToken = bson.Raw(typedMsg)
				case recheckBatch:
					allEvents = append(allEvents, typedMsg...)
				}
			}

			if srcToken == nil || dstToken == nil {
				continue
			}

			dbNames := make([]string, 0, len(allEvents))
			collNames := make([]string, 0, len(allEvents))
			docIDs := make([]bson.RawValue, 0, len(allEvents))
			dataSizes := make([]int, 0, len(allEvents))

			for _, changeEvent := range allEvents {
				dbNames = append(dbNames, changeEvent.Ns.DB)
				collNames = append(collNames, changeEvent.Ns.Coll)
				docIDs = append(docIDs, changeEvent.DocID)

				var size int
				if changeEvent.FullDocLen.OrZero() > 0 {
					size = int(changeEvent.FullDocLen.OrZero())
				} else if changeEvent.FullDocument == nil {
					// This happens for deletes and for some updates.
					// The document is probably, but not necessarily, deleted.
					size = fauxDocSizeForDeleteEvents
				} else {
					// This happens for inserts, replaces, and most updates.
					size = len(changeEvent.FullDocument)
				}

				dataSizes = append(dataSizes, size)
			}

			err := v.insertRecheckDocs(
				ctx,
				dbNames,
				collNames,
				docIDs,
				dataSizes,
			)
			if err != nil {
				errEventual.Set(errors.Wrapf(err, "persisting rechecks"))
				return
			}

			err = persistChangeStreamResumeToken(
				ctx,
				metaColl,
				srcResumeTokenDocID,
				srcToken,
			)
			if err != nil {
				errEventual.Set(errors.Wrap(err, "persisting source resume token"))
				return
			}

			err = persistChangeStreamResumeToken(
				ctx,
				metaColl,
				dstResumeTokenDocID,
				dstToken,
			)
			if err != nil {
				errEventual.Set(errors.Wrap(err, "persisting destination resume token"))
				return
			}
		}
	}()

	return inChan, errEventual
}

/*
func logResumeToken(
	logger *logger.Logger,
	whichReader string,
	token bson.Raw,
) error {
	ts, err := extractTimestampFromResumeToken(token)

	logEvent := logger.Debug()

	if err == nil {
		logEvent = addTimestampToLogEvent(ts, logEvent)
	} else {
		logger.Warn().Err(err).
			Msg("failed to extract resume token timestamp")
	}

	logEvent.Msgf("Persisted %s's resume token.", whichReader)
}
*/

func persistChangeStreamResumeToken(
	ctx context.Context,
	coll *mongo.Collection,
	docID string,
	token bson.Raw,
) error {
	_, err := coll.ReplaceOne(
		ctx,
		bson.D{{"_id", docID}},
		token,
		options.Replace().SetUpsert(true),
	)

	return err
}
