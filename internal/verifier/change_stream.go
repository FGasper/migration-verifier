package verifier

import (
	"context"
	"fmt"
	"time"

	"github.com/10gen/migration-verifier/internal/keystring"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/exp/constraints"
)

// ParsedEvent contains the fields of an event that we have parsed from 'bson.Raw'.
type ParsedEvent struct {
	ID          interface{}          `bson:"_id"`
	OpType      string               `bson:"operationType"`
	Ns          *Namespace           `bson:"ns,omitempty"`
	DocKey      DocKey               `bson:"documentKey,omitempty"`
	DocSize     *int                 `bson:"documentSize,omitempty"`
	ClusterTime *primitive.Timestamp `bson:"clusterTime,omitEmpty"`
}

func (pe *ParsedEvent) String() string {
	return fmt.Sprintf("{OpType: %s, namespace: %s, docID: %v, clusterTime: %v}", pe.OpType, pe.Ns, pe.DocKey.ID, pe.ClusterTime)
}

// DocKey is a deserialized form for the ChangeEvent documentKey field. We currently only care about
// the _id.
type DocKey struct {
	ID interface{} `bson:"_id"`
}

const (
	minChangeStreamPersistInterval     = time.Second * 10
	metadataChangeStreamCollectionName = "changeStream"
)

type UnknownEventError struct {
	Event *ParsedEvent
}

func (uee UnknownEventError) Error() string {
	return fmt.Sprintf("Received event with unknown optype: %+v", uee.Event)
}

// HandleChangeStreamEvents performs the necessary work for change stream events after receiving a batch.
func (verifier *Verifier) HandleChangeStreamEvents(ctx context.Context, batch []ParsedEvent) error {
	if len(batch) == 0 {
		return nil
	}

	dbNames := make([]string, len(batch))
	collNames := make([]string, len(batch))
	docIDs := make([]interface{}, len(batch))
	dataSizes := make([]int, len(batch))

	for i, changeEvent := range batch {
		if changeEvent.ClusterTime != nil &&
			(verifier.lastChangeEventTime == nil ||
				verifier.lastChangeEventTime.Compare(*changeEvent.ClusterTime) < 0) {
			verifier.lastChangeEventTime = changeEvent.ClusterTime
		}
		switch changeEvent.OpType {
		case "delete":
			fallthrough
		case "insert":
			fallthrough
		case "replace":
			fallthrough
		case "update":
			if err := verifier.eventRecorder.AddEvent(&changeEvent); err != nil {
				return errors.Wrapf(err, "failed to augment stats with change event: %+v", changeEvent)
			}
			dbNames[i] = changeEvent.Ns.DB
			collNames[i] = changeEvent.Ns.Coll
			docIDs[i] = changeEvent.DocKey.ID

			if changeEvent.DocSize == nil {
				// This happens for deletes and for some updates.
				// The document is probably, but not necessarily, deleted.
				dataSizes[i] = 1024
			} else {
				// This happens for inserts, replaces, and most updates.
				dataSizes[i] = *changeEvent.DocSize
			}
		default:
			return UnknownEventError{Event: &changeEvent}
		}
	}

	verifier.logger.Debug().
		Int("count", len(docIDs)).
		Msg("Persisting rechecks for change events.")

	return verifier.insertRecheckDocs(ctx, dbNames, collNames, docIDs, dataSizes)
}

func (verifier *Verifier) GetChangeStreamFilter() []bson.D {
	var pipeline mongo.Pipeline

	if len(verifier.srcNamespaces) == 0 {
		pipeline = []bson.D{{bson.E{"$match", bson.D{{"ns.db", bson.D{{"$ne", verifier.metaDBName}}}}}}}
	} else {
		filter := bson.A{}
		for _, ns := range verifier.srcNamespaces {
			db, coll := SplitNamespace(ns)
			filter = append(filter, bson.D{{"ns", bson.D{{"db", db}, {"coll", coll}}}})
		}
		stage := bson.D{{"$match", bson.D{{"$or", filter}}}}
		pipeline = []bson.D{stage}
	}

	return append(
		pipeline,
		[]bson.D{

			// Add a documentSize field.
			{
				{"$addFields", bson.D{
					{"documentSize", bson.D{
						{"$cond", bson.D{
							{"$if", bson.D{
								{"$ne", bson.A{
									"missing",
									bson.D{{"$type", "$fullDocument"}},
								}},
							}}, // fullDocument exists
							{"then", bson.D{{"$binarySize", "$fullDocument"}}},
							{"else", "$$REMOVE"},
						}},
					}},
				}},
			},

			// Remove the fullDocument field since a) we don't use it, and
			// b) it's big.
			{
				{"$project", bson.D{
					{"fullDocument", 0},
				}},
			},
		}...,
	)
}

func (verifier *Verifier) iterateChangeStream(ctx mongo.SessionContext, cs *mongo.ChangeStream) {
	var lastPersistedTime time.Time

	persistResumeTokenIfNeeded := func() error {
		if time.Since(lastPersistedTime) <= minChangeStreamPersistInterval {
			return nil
		}

		err := verifier.persistChangeStreamResumeToken(ctx, cs)
		if err == nil {
			lastPersistedTime = time.Now()
		}

		return err
	}

	readAndHandleOneChangeEventBatch := func() (bool, error) {
		eventsRead := 0
		var changeEventBatch []ParsedEvent

		for hasEventInBatch := true; hasEventInBatch; hasEventInBatch = cs.RemainingBatchLength() > 0 {
			gotEvent := cs.TryNext(ctx)

			if !gotEvent || cs.Err() != nil {
				break
			}

			if changeEventBatch == nil {
				changeEventBatch = make([]ParsedEvent, cs.RemainingBatchLength()+1)
			}

			if err := cs.Decode(&changeEventBatch[eventsRead]); err != nil {
				return false, errors.Wrap(err, "failed to decode change event")
			}

			eventsRead++
		}

		if eventsRead > 0 {
			verifier.logger.Debug().Int("eventsCount", eventsRead).Msgf("Received a batch of events.")
			err := verifier.HandleChangeStreamEvents(ctx, changeEventBatch)
			if err != nil {
				return false, errors.Wrap(err, "failed to handle change events")
			}
		}

		return eventsRead > 0, errors.Wrap(cs.Err(), "change stream iteration failed")
	}

	for {
		var err error
		var changeStreamEnded bool

		select {

		// If the context is canceled, return immmediately.
		case <-ctx.Done():
			return

		// If the changeStreamEnderChan has a message, the user has indicated that
		// source writes are ended. This means we should exit rather than continue
		// reading the change stream since there should be no more events.
		case <-verifier.changeStreamEnderChan:
			verifier.logger.Debug().
				Msg("Change stream thread received shutdown request.")

			changeStreamEnded = true

			// Read all change events until the source reports no events.
			// (i.e., the `getMore` call returns empty)
			for {
				var gotEvent bool
				gotEvent, err = readAndHandleOneChangeEventBatch()

				if !gotEvent || err != nil {
					break
				}
			}

		default:
			_, err = readAndHandleOneChangeEventBatch()
		}

		if err == nil {
			err = persistResumeTokenIfNeeded()
		}

		if err != nil && !errors.Is(err, context.Canceled) {
			verifier.logger.Debug().
				Err(err).
				Msg("Sending change stream error.")

			verifier.changeStreamErrChan <- err

			if !changeStreamEnded {
				break
			}
		}

		if changeStreamEnded {
			verifier.mux.Lock()
			verifier.changeStreamRunning = false
			if verifier.lastChangeEventTime != nil {
				verifier.srcStartAtTs = verifier.lastChangeEventTime
			}
			verifier.mux.Unlock()
			// since we have started Recheck, we must signal that we have
			// finished the change stream changes so that Recheck can continue.
			verifier.changeStreamDoneChan <- struct{}{}
			break
		}
	}

	infoLog := verifier.logger.Info()
	if verifier.lastChangeEventTime == nil {
		infoLog = infoLog.Str("changeStreamStopTime", "none")
	} else {
		infoLog = infoLog.Interface("changeStreamStopTime", *verifier.lastChangeEventTime)
	}

	infoLog.Msg("Change stream is done.")
}

// StartChangeStream starts the change stream.
func (verifier *Verifier) StartChangeStream(ctx context.Context) error {
	pipeline := verifier.GetChangeStreamFilter()
	opts := options.ChangeStream().
		SetFullDocument(options.UpdateLookup).
		SetMaxAwaitTime(1 * time.Second)

	savedResumeToken, err := verifier.loadChangeStreamResumeToken(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to load persisted change stream resume token")
	}

	csStartLogEvent := verifier.logger.Info()

	if savedResumeToken != nil {
		logEvent := csStartLogEvent.
			Stringer("resumeToken", savedResumeToken)

		ts, err := extractTimestampFromResumeToken(savedResumeToken)
		if err == nil {
			logEvent = addUnixTimeToLogEvent(ts.T, logEvent, "timestamp")
		} else {
			verifier.logger.Warn().
				Err(err).
				Msg("Failed to extract timestamp from persisted resume token.")
		}

		logEvent.Msg("Starting change stream from persisted resume token.")

		opts = opts.SetStartAfter(savedResumeToken)
	} else {
		csStartLogEvent.Msg("Starting change stream from current source cluster time.")
	}

	sess, err := verifier.srcClient.StartSession()
	if err != nil {
		return errors.Wrap(err, "failed to start session")
	}
	sctx := mongo.NewSessionContext(ctx, sess)
	srcChangeStream, err := verifier.srcClient.Watch(sctx, pipeline, opts)
	if err != nil {
		return errors.Wrap(err, "failed to open change stream")
	}

	err = verifier.persistChangeStreamResumeToken(sctx, srcChangeStream)
	if err != nil {
		return err
	}

	csTimestamp, err := extractTimestampFromResumeToken(srcChangeStream.ResumeToken())
	if err != nil {
		return errors.Wrap(err, "failed to extract timestamp from change stream's resume token")
	}

	clusterTime, err := getClusterTimeFromSession(sess)
	if err != nil {
		return errors.Wrap(err, "failed to read cluster time from session")
	}

	verifier.srcStartAtTs = &csTimestamp
	if csTimestamp.After(clusterTime) {
		verifier.srcStartAtTs = &clusterTime
	}

	verifier.mux.Lock()
	verifier.changeStreamRunning = true
	verifier.mux.Unlock()

	go verifier.iterateChangeStream(
		mongo.NewSessionContext(ctx, sess),
		srcChangeStream,
	)

	return nil
}

func addUnixTimeToLogEvent[T constraints.Integer](unixTime T, event *zerolog.Event, name string) *zerolog.Event {
	return event.Time(name, time.Unix(int64(unixTime), int64(0)))
}

func (v *Verifier) getChangeStreamMetadataCollection() *mongo.Collection {
	return v.metaClient.Database(v.metaDBName).Collection(metadataChangeStreamCollectionName)
}

func (verifier *Verifier) loadChangeStreamResumeToken(ctx context.Context) (bson.Raw, error) {
	coll := verifier.getChangeStreamMetadataCollection()

	token, err := coll.FindOne(
		ctx,
		bson.D{{"_id", "resumeToken"}},
	).Raw()

	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}

	return token, err
}

func (verifier *Verifier) persistChangeStreamResumeToken(ctx mongo.SessionContext, cs *mongo.ChangeStream) error {
	token := cs.ResumeToken()

	coll := verifier.getChangeStreamMetadataCollection()
	_, err := coll.ReplaceOne(
		ctx,
		bson.D{{"_id", "resumeToken"}},
		token,
		options.Replace().SetUpsert(true),
	)

	if err == nil {
		ts, err := extractTimestampFromResumeToken(token)

		logEvent := verifier.logger.Debug()

		var hasResumeTokenTimestamp bool
		if err == nil {
			hasResumeTokenTimestamp = true
			logEvent = addUnixTimeToLogEvent(ts.T, logEvent, "timestamp")
		} else {
			verifier.logger.Warn().Err(err).
				Msg("failed to extract resume token timestamp")
		}

		clusterTime, err := getClusterTimeFromSession(ctx)
		if err == nil {
			logEvent = addUnixTimeToLogEvent(clusterTime.T, logEvent, "clusterTime")

			if hasResumeTokenTimestamp {
				lag := time.Second * time.Duration(clusterTime.T-ts.T)

				if lag > 30*time.Minute {
					verifier.logger.Warn().
						Stringer("lag", lag).
						Msg("Lag time seems excessive. This data may change faster than migration-verifier can enqueue rechecks. Please minimize changes to the data during verification.")
				}
				logEvent = logEvent.Stringer("lag", lag)
			}
		} else {
			verifier.logger.Warn().Err(err).
				Msg("failed to extract cluster time from change stream session")
		}

		logEvent.Msg("Persisted change stream resume token.")

		return nil
	}

	return errors.Wrapf(err, "failed to persist change stream resume token (%v)", token)
}

func extractTimestampFromResumeToken(resumeToken bson.Raw) (primitive.Timestamp, error) {
	tokenStruct := struct {
		Data string `bson:"_data"`
	}{}

	// Change stream token is always a V1 keystring in the _data field
	err := bson.Unmarshal(resumeToken, &tokenStruct)
	if err != nil {
		return primitive.Timestamp{}, errors.Wrapf(err, "failed to extract %#q from resume token (%v)", "_data", resumeToken)
	}

	resumeTokenBson, err := keystring.KeystringToBson(keystring.V1, tokenStruct.Data)
	if err != nil {
		return primitive.Timestamp{}, err
	}
	// First element is the cluster time we want
	resumeTokenTime, ok := resumeTokenBson[0].Value.(primitive.Timestamp)
	if !ok {
		return primitive.Timestamp{}, errors.Errorf("resume token data's (%+v) first element is of type %T, not a timestamp", resumeTokenBson, resumeTokenBson[0].Value)
	}

	return resumeTokenTime, nil
}

func getClusterTimeFromSession(sess mongo.Session) (primitive.Timestamp, error) {
	ctStruct := struct {
		ClusterTime struct {
			ClusterTime primitive.Timestamp `bson:"clusterTime"`
		} `bson:"$clusterTime"`
	}{}

	clusterTimeRaw := sess.ClusterTime()
	err := bson.Unmarshal(sess.ClusterTime(), &ctStruct)
	if err != nil {
		return primitive.Timestamp{}, errors.Wrapf(err, "failed to find clusterTime in session cluster time document (%v)", clusterTimeRaw)
	}

	return ctStruct.ClusterTime.ClusterTime, nil
}
