package verifier

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/10gen/migration-verifier/contextplus"
	"github.com/10gen/migration-verifier/internal/retry"
	"github.com/10gen/migration-verifier/internal/types"
	"github.com/10gen/migration-verifier/internal/util"
	"github.com/10gen/migration-verifier/mbson"
	"github.com/10gen/migration-verifier/mslices"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"golang.org/x/exp/slices"
)

const readTimeout = 10 * time.Minute

func (verifier *Verifier) FetchAndCompareDocuments(
	givenCtx context.Context,
	task *VerificationTask,
) (
	[]VerificationResult,
	types.DocumentCount,
	types.ByteCount,
	error,
) {
	var srcChannel, dstChannel <-chan bson.Raw
	var readSrcCallback, readDstCallback func(context.Context, *retry.FuncInfo) error

	results := []VerificationResult{}
	var docCount types.DocumentCount
	var byteCount types.ByteCount

	retryer := retry.New().WithDescription(
		"comparing task %v's documents (namespace: %s)",
		task.PrimaryKey,
		task.QueryFilter.Namespace,
	)

	err := retryer.
		WithBefore(func() {
			srcChannel, dstChannel, readSrcCallback, readDstCallback = verifier.getFetcherChannelsAndCallbacks(task)
		}).
		WithErrorCodes(util.CursorKilledErrCode).
		WithCallback(
			func(ctx context.Context, fi *retry.FuncInfo) error {
				return readSrcCallback(ctx, fi)
			},
			"reading from source",
		).
		WithCallback(
			func(ctx context.Context, fi *retry.FuncInfo) error {
				return readDstCallback(ctx, fi)
			},
			"reading from destination",
		).
		WithCallback(
			func(ctx context.Context, fi *retry.FuncInfo) error {
				var err error
				results, docCount, byteCount, err = verifier.compareDocsFromChannels(
					ctx,
					fi,
					task,
					srcChannel,
					dstChannel,
				)

				return err
			},
			"comparing documents",
		).Run(givenCtx, verifier.logger)

	return results, docCount, byteCount, err
}

func (verifier *Verifier) compareDocsFromChannels(
	ctx context.Context,
	fi *retry.FuncInfo,
	task *VerificationTask,
	srcChannel, dstChannel <-chan bson.Raw,
) (
	[]VerificationResult,
	types.DocumentCount,
	types.ByteCount,
	error,
) {
	results := []VerificationResult{}
	var srcDocCount types.DocumentCount
	var srcByteCount types.ByteCount

	mapKeyFieldNames := task.QueryFilter.GetDocKeyFieldNames()

	namespace := task.QueryFilter.Namespace

	srcCache := map[string]bson.Raw{}
	dstCache := map[string]bson.Raw{}

	// This is the core document-handling logic. It either:
	//
	// a) caches the new document if its mapKey is unseen, or
	// b) compares the new doc against its previously-received, cached
	//    counterpart and records any mismatch.
	handleNewDoc := func(doc bson.Raw, isSrc bool) error {
		var mapKey string

		if verifier.shouldCompareFullDocuments() {
			mapKey = getMapKey(doc, mapKeyFieldNames)
		} else {
			found, err := mbson.RawLookup(doc, &mapKey, "docKey")
			if err != nil {
				return errors.Wrapf(err, "failed to extract %#q", "docKey")
			}
			if !found {
				return errors.Errorf("No %#q found in document: %+v", "docKey", doc)
			}
		}

		var ourMap, theirMap map[string]bson.Raw

		if isSrc {
			ourMap = srcCache
			theirMap = dstCache
		} else {
			ourMap = dstCache
			theirMap = srcCache
		}
		// See if we've already cached a document with this
		// mapKey from the other channel.
		theirDoc, exists := theirMap[mapKey]

		// If there is no such cached document, then cache the newly-received
		// document in our map then proceed to the next document.
		//
		// (We'll remove the cache entry when/if the other channel yields a
		// document with the same mapKey.)
		if !exists {
			ourMap[mapKey] = doc
			return nil
		}

		// We have two documents! First we remove the cache entry. This saves
		// memory, but more importantly, it lets us know, once we exhaust the
		// channels, which documents were missing on one side or the other.
		delete(theirMap, mapKey)

		// Now we determine which document came from whom.
		var srcDoc, dstDoc bson.Raw
		if isSrc {
			srcDoc = doc
			dstDoc = theirDoc
		} else {
			srcDoc = theirDoc
			dstDoc = doc
		}

		// Finally we compare the documents and save any mismatch report(s).
		mismatches, err := verifier.compareOneDocument(srcDoc, dstDoc, namespace)
		if err != nil {
			return errors.Wrap(err, "failed to compare documents")
		}

		results = append(results, mismatches...)

		return nil
	}

	var srcClosed, dstClosed bool

	readTimer := time.NewTimer(0)
	defer func() {
		if !readTimer.Stop() {
			<-readTimer.C
		}
	}()

	// We always read src & dst together. This ensures that, if one side
	// lags the other significantly, we won’t keep caching the faster side’s
	// documents and thus consume more & more memory.
	for !srcClosed || !dstClosed {
		simpleTimerReset(readTimer, readTimeout)

		var srcDoc, dstDoc bson.Raw

		eg, egCtx := contextplus.ErrGroup(ctx)

		if !srcClosed {
			eg.Go(func() error {
				var alive bool
				select {
				case <-egCtx.Done():
					return egCtx.Err()
				case <-readTimer.C:
					return errors.Errorf(
						"failed to read from source after %s",
						readTimeout,
					)
				case srcDoc, alive = <-srcChannel:
					if !alive {
						srcClosed = true
						break
					}

					fi.NoteSuccess("received document from source")

					srcDocCount++
					srcByteCount += types.ByteCount(len(srcDoc))
				}

				return nil
			})
		}

		if !dstClosed {
			eg.Go(func() error {
				var alive bool
				select {
				case <-egCtx.Done():
					return egCtx.Err()
				case <-readTimer.C:
					return errors.Errorf(
						"failed to read from destination after %s",
						readTimeout,
					)
				case dstDoc, alive = <-dstChannel:
					if !alive {
						dstClosed = true
						break
					}

					fi.NoteSuccess("received document from destination")
				}

				return nil
			})
		}

		if err := eg.Wait(); err != nil {
			return nil, 0, 0, errors.Wrap(
				err,
				"failed to read documents",
			)
		}

		if srcDoc != nil {
			err := handleNewDoc(srcDoc, true)

			if err != nil {
				return nil, 0, 0, errors.Wrapf(
					err,
					"comparer thread failed to handle %#q's source doc (task: %s) with ID %v",
					namespace,
					task.PrimaryKey,
					srcDoc.Lookup("_id"),
				)
			}
		}

		if dstDoc != nil {
			err := handleNewDoc(dstDoc, false)

			if err != nil {
				return nil, 0, 0, errors.Wrapf(
					err,
					"comparer thread failed to handle %#q's destination doc (task: %s) with ID %v",
					namespace,
					task.PrimaryKey,
					dstDoc.Lookup("_id"),
				)
			}
		}
	}

	// We got here because both srcChannel and dstChannel are closed,
	// which means we have processed all documents with the same mapKey
	// between source & destination.
	//
	// At this point, any documents left in the cache maps are simply
	// missing on the other side. We add results for those.

	// We might as well pre-grow the slice:
	results = slices.Grow(results, len(srcCache)+len(dstCache))

	for _, doc := range srcCache {
		results = append(
			results,
			VerificationResult{
				ID:        doc.Lookup("_id"),
				Details:   Missing,
				Cluster:   ClusterTarget,
				NameSpace: namespace,
				dataSize:  len(doc),
			},
		)
	}

	for _, doc := range dstCache {
		results = append(
			results,
			VerificationResult{
				ID:        doc.Lookup("_id"),
				Details:   Missing,
				Cluster:   ClusterSource,
				NameSpace: namespace,
				dataSize:  len(doc),
			},
		)
	}

	return results, srcDocCount, srcByteCount, nil
}

func simpleTimerReset(t *time.Timer, dur time.Duration) {
	if !t.Stop() {
		<-t.C
	}

	t.Reset(dur)
}

func (verifier *Verifier) getFetcherChannelsAndCallbacks(
	task *VerificationTask,
) (
	<-chan bson.Raw,
	<-chan bson.Raw,
	func(context.Context, *retry.FuncInfo) error,
	func(context.Context, *retry.FuncInfo) error,
) {
	srcChannel := make(chan bson.Raw)
	dstChannel := make(chan bson.Raw)

	readSrcCallback := func(ctx context.Context, state *retry.FuncInfo) error {
		cursor, err := verifier.getDocumentsCursor(
			ctx,
			verifier.srcClientCollection(task),
			verifier.srcClusterInfo,
			verifier.srcChangeStreamReader.startAtTs,
			task,
		)

		if err == nil {
			state.NoteSuccess("opened src find cursor")

			err = errors.Wrap(
				iterateCursorToChannel(ctx, state, cursor, srcChannel),
				"failed to read source documents",
			)
		} else {
			err = errors.Wrap(
				err,
				"failed to find source documents",
			)
		}

		return err
	}

	readDstCallback := func(ctx context.Context, state *retry.FuncInfo) error {
		cursor, err := verifier.getDocumentsCursor(
			ctx,
			verifier.dstClientCollection(task),
			verifier.dstClusterInfo,
			verifier.dstChangeStreamReader.startAtTs,
			task,
		)

		if err == nil {
			state.NoteSuccess("opened dst find cursor")

			err = errors.Wrap(
				iterateCursorToChannel(ctx, state, cursor, dstChannel),
				"failed to read destination documents",
			)
		} else {
			err = errors.Wrap(
				err,
				"failed to find destination documents",
			)
		}

		return err
	}

	return srcChannel, dstChannel, readSrcCallback, readDstCallback
}

func (verifier *Verifier) getDocumentsCursor(
	ctx context.Context,
	collection *mongo.Collection,
	clusterInfo *util.ClusterInfo,
	startAtTs *primitive.Timestamp,
	task *VerificationTask,
) (*mongo.Cursor, error) {
	runCommandOptions := options.RunCmd()
	var andPredicates bson.A

	if len(task.Ids) > 0 {
		andPredicates = append(andPredicates, bson.D{{"_id", bson.M{"$in": task.Ids}}})
	}
	andPredicates = verifier.maybeAppendGlobalFilterToPredicates(andPredicates)

	var cmd bson.D
	var cmdOpts bson.D

	if verifier.readPreference.Mode() != readpref.PrimaryMode {
		runCommandOptions = runCommandOptions.SetReadPreference(verifier.readPreference)

		if startAtTs != nil {

			// We never want to read before the change stream start time,
			// or for the last generation, the change stream end time.
			cmdOpts = append(
				cmdOpts,
				bson.E{"readConcern", bson.D{
					{"afterClusterTime", *startAtTs},
				}},
			)
		}
	}

	if verifier.shouldCompareFullDocuments() {
		var findOptions bson.D

		if len(task.Ids) > 0 {
			findOptions = bson.D{
				bson.E{"filter", bson.D{{"$and", andPredicates}}},
			}
		} else {
			findOptions = task.QueryFilter.Partition.GetFindOptions(clusterInfo, verifier.maybeAppendGlobalFilterToPredicates(nil))
		}

		findOptions = append(findOptions, cmdOpts...)

		cmd = append(bson.D{{"find", collection.Name()}}, findOptions...)
	} else {
		var pl mongo.Pipeline

		if len(task.Ids) > 0 {
			pl = mongo.Pipeline{
				{{"$match", bson.D{{"$and", andPredicates}}}},
			}
		} else {
			pl = task.QueryFilter.Partition.GetAggregationStages(
				clusterInfo,
				verifier.maybeAppendGlobalFilterToPredicates(nil),
				task.QueryFilter.GetDocKeyFieldNames(),
			)
		}

		cmd = append(
			bson.D{
				{"aggregate", collection.Name()},
				{"pipeline", pl},
			},
			cmdOpts...,
		)
	}

	// Suppress this log for recheck tasks because the list of IDs can be
	// quite long.
	if len(task.Ids) == 0 {
		verifier.logger.Debug().
			Any("task", task.PrimaryKey).
			Str("cmd", fmt.Sprintf("%s", cmd)).
			Str("options", fmt.Sprintf("%v", *runCommandOptions)).
			Msg("getDocuments command.")
	}

	return collection.Database().RunCommandCursor(ctx, cmd, runCommandOptions)
}

func (v *Verifier) shouldCompareFullDocuments() bool {
	if v.ignoreBSONFieldOrder {
		return true
	}

	for _, clusterInfo := range mslices.Of(v.srcClusterInfo, v.dstClusterInfo) {
		if clusterInfo.VersionArray[0] < 4 {
			return true
		}

		if clusterInfo.VersionArray[0] == 4 && clusterInfo.VersionArray[1] < 4 {
			return true
		}
	}

	return false
}

func iterateCursorToChannel(
	ctx context.Context,
	state *retry.FuncInfo,
	cursor *mongo.Cursor,
	writer chan<- bson.Raw,
) error {
	defer close(writer)

	for cursor.Next(ctx) {
		state.NoteSuccess("received a document")

		select {
		case <-ctx.Done():
			return ctx.Err()
		case writer <- slices.Clone(cursor.Current):
			state.NoteSuccess("sent document to compare thread")
		}
	}

	return errors.Wrap(cursor.Err(), "failed to iterate cursor")
}

func getMapKey(doc bson.Raw, fieldNames []string) string {
	var keyBuffer bytes.Buffer
	for _, keyName := range fieldNames {
		value := doc.Lookup(keyName)
		keyBuffer.Grow(1 + len(value.Value))
		keyBuffer.WriteByte(byte(value.Type))
		keyBuffer.Write(value.Value)
	}

	return keyBuffer.String()
}
