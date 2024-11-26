package verifier

import (
	"context"
	"time"

	"github.com/10gen/migration-verifier/internal/types"
	"github.com/10gen/migration-verifier/internal/util"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"golang.org/x/sync/errgroup"
)

func (verifier *Verifier) FetchAndCompareDocuments(
	ctx context.Context,
	task *VerificationTask,
) (
	[]VerificationResult,
	types.DocumentCount,
	types.ByteCount,
	error,
) {
	results := []VerificationResult{}
	var docCount types.DocumentCount
	var byteCount types.ByteCount

	mapKeyFieldNames := make([]string, 1+len(task.QueryFilter.ShardKeys))
	mapKeyFieldNames[0] = "_id"
	copy(mapKeyFieldNames[1:], task.QueryFilter.ShardKeys)

	namespace := task.QueryFilter.Namespace

	srcCache := map[string]bson.Raw{}
	dstCache := map[string]bson.Raw{}

	// This is the core document-handling logic. It either:
	//
	// a) caches the new document if its mapKey is unseen, or
	// b) compares the new doc against its previously-received, cached
	//    counterpart and records any mismatch.
	handleNewDoc := func(doc bson.Raw, isSrc bool) error {
		mapKey := getMapKey(doc, mapKeyFieldNames)

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

	readTimer := time.NewTimer(0)
	defer func() {
		if !readTimer.Stop() {
			<-readTimer.C
		}
	}()

	srcCursor, dstCursor, err := verifier.getCursorsForTask(ctx, task)
	if err != nil {
		return nil, 0, 0, errors.Wrap(err, "failed to find task %s’s documents")
	}
	defer srcCursor.Close(ctx)
	defer dstCursor.Close(ctx)

	var srcDone, dstDone bool
	for !srcDone || !dstDone {
		eg, egCtx := errgroup.WithContext(ctx)

		if !srcDone {
			eg.Go(func() error {
				var batch []bson.Raw
				err := util.TryNextBatch(
					egCtx,
					srcCursor,
					&batch,
				)

				if err != nil {
					return errors.Wrapf(
						err,
						"failed to read task %s’s source documents",
						task.PrimaryKey,
					)
				}

				for _, doc := range batch {
					handleNewDoc(doc, true)
				}

				if len(batch) == 0 {
					srcDone = true
				}

				return nil
			})
		}

		if !dstDone {
			eg.Go(func() error {
				var batch []bson.Raw
				err := util.TryNextBatch(
					egCtx,
					dstCursor,
					&batch,
				)

				if err != nil {
					return errors.Wrapf(
						err,
						"failed to read task %s’s destination documents",
						task.PrimaryKey,
					)
				}

				for _, doc := range batch {
					handleNewDoc(doc, false)
				}

				if len(batch) == 0 {
					dstDone = true
				}

				return nil
			})

		}

		err := eg.Wait()
		if err != nil {
			return nil, 0, 0, errors.Wrapf(
				err,
				"failed to compare task %s’s documents",
				task.PrimaryKey,
			)
		}
	}

	return results, docCount, byteCount, nil
}

func (verifier *Verifier) getCursorsForTask(
	ctx context.Context,
	task *VerificationTask,
) (
	srcCursor, dstCursor *mongo.Cursor,
	err error,
) {
	eg, egCtx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		var err error
		srcCursor, err = verifier.getDocumentsCursor(
			egCtx,
			verifier.srcClientCollection(task),
			verifier.srcBuildInfo,
			verifier.srcStartAtTs,
			task,
		)

		return errors.Wrapf(
			err,
			"failed to find task %s’s source documents",
			task.PrimaryKey,
		)
	})

	eg.Go(func() error {
		var err error
		dstCursor, err = verifier.getDocumentsCursor(
			egCtx,
			verifier.dstClientCollection(task),
			verifier.srcBuildInfo,
			nil, // startAtTs
			task,
		)

		return errors.Wrapf(
			err,
			"failed to find task %s’s destination documents",
			task.PrimaryKey,
		)
	})

	err = eg.Wait()

	return
}
