package verifier

import (
	"context"

	"github.com/10gen/migration-verifier/internal/logger"
	"github.com/10gen/migration-verifier/internal/retry"
	"github.com/10gen/migration-verifier/option"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	mismatchesCollectionName = "mismatches"
)

type MismatchInfo struct {
	Task   primitive.ObjectID
	Detail VerificationResult
}

func createMismatchesCollection(ctx context.Context, db *mongo.Database) error {
	_, err := db.Collection(mismatchesCollectionName).Indexes().CreateMany(
		ctx,
		[]mongo.IndexModel{
			{
				Keys: bson.D{
					{"task", 1},
				},
			},
		},
	)

	if err != nil {
		return errors.Wrapf(err, "creating indexes for collection %#q", mismatchesCollectionName)
	}

	return nil
}

type perTaskMismatchCounts struct {
	Task             primitive.ObjectID
	Mismatched       int
	MissingOrChanged int
}

func getPerTaskMismatchCounts(
	ctx context.Context,
	db *mongo.Database,
	taskIDs []primitive.ObjectID,
) ([]perTaskMismatchCounts, error) {
	cursor, err := db.Collection(mismatchesCollectionName).Aggregate(
		ctx,
		mongo.Pipeline{
			{{"$match", bson.D{
				{"task", bson.D{{"$in", taskIDs}}},
			}}},
			{{"$project", bson.D{
				{"task", 1},
				{"detail.details", 1},
				{"detail.field", 1},
			}}},
			{{"$addFields", bson.D{
				{"_missing", bson.D{
					{"$cond", bson.D{
						{"if", bson.D{{"$and", []bson.D{
							{{"$eq", []any{"$detail.details", Missing}}},
							{{"$eq", []any{"$detail.field", ""}}},
						}}}},
						{"then", 1},
						{"else", 0},
					}},
				}},
				{"detail", "$$REMOVE"},
			}}},
			{{"$group", bson.D{
				{"_id", "$task"},
				{"missing", bson.D{{"$sum", "$_missing"}}},
				{"mismatch", bson.D{{"$sum", bson.D{{"$subtract", []any{
					1, "$_missing",
				}}}}}},
			}}},
		},
	)
	if err != nil {
		return nil, errors.Wrapf(err, "requesting counts of mismatches & missing for %d task(s)", len(taskIDs))
	}

	var ret []perTaskMismatchCounts

	if err = cursor.All(ctx, &ret); err != nil {
		return nil, errors.Wrapf(err, "fetching counts of mismatches & missing for %d task(s)", len(taskIDs))
	}

	return ret, nil
}

func getMismatchesForTasks(
	ctx context.Context,
	db *mongo.Database,
	taskIDs []primitive.ObjectID,
	limit option.Option[int],
) (map[primitive.ObjectID][]VerificationResult, error) {
	findOpts := options.Find().SetSort(
		bson.D{
			{"detail.id", 1},
		},
	)

	if limit, hasLimit := limit.Get(); hasLimit {
		findOpts.SetLimit(int64(limit))
	}

	cursor, err := db.Collection(mismatchesCollectionName).Find(
		ctx,
		bson.D{
			{"task", bson.D{{"$in", taskIDs}}},
		},
		findOpts,
	)

	if err != nil {
		return nil, errors.Wrapf(err, "fetching %d tasks' discrepancies", len(taskIDs))
	}

	result := map[primitive.ObjectID][]VerificationResult{}

	for cursor.Next(ctx) {
		if cursor.Err() != nil {
			break
		}

		var d MismatchInfo
		if err := cursor.Decode(&d); err != nil {
			return nil, errors.Wrapf(err, "parsing discrepancy %+v", cursor.Current)
		}

		result[d.Task] = append(
			result[d.Task],
			d.Detail,
		)
	}

	if cursor.Err() != nil {
		return nil, errors.Wrapf(err, "reading %d tasks' discrepancies", len(taskIDs))
	}

	for _, taskID := range taskIDs {
		if _, ok := result[taskID]; !ok {
			result[taskID] = []VerificationResult{}
		}
	}

	return result, nil
}

func recordMismatches(
	ctx context.Context,
	logger *logger.Logger,
	db *mongo.Database,
	taskID primitive.ObjectID,
	problems []VerificationResult,
) error {
	if option.IfNotZero(taskID).IsNone() {
		panic("empty task ID given")
	}

	models := lo.Map(
		problems,
		func(r VerificationResult, _ int) mongo.WriteModel {
			return &mongo.InsertOneModel{
				Document: MismatchInfo{
					Task:   taskID,
					Detail: r,
				},
			}
		},
	)

	return retry.New().WithCallback(
		func(ctx context.Context, _ *retry.FuncInfo) error {
			_, err := db.Collection(mismatchesCollectionName).BulkWrite(
				ctx,
				models,
			)

			return err
		},
		"recording %d mismatches",
		len(models),
	).Run(ctx, logger)
}
