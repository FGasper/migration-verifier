package partitions

import (
	"fmt"
	"slices"

	"github.com/10gen/migration-verifier/internal/util"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// PartitionKey represents the _id of a partition document stored in the destination.
type PartitionKey struct {
	SourceUUID  util.UUID `bson:"srcUUID"`
	MongosyncID string    `bson:"id"`
	Lower       any       `bson:"lowerBound"`
}

// Namespace stores the database and collection name of the namespace being copied.
type Namespace struct {
	DB   string `bson:"db"`
	Coll string `bson:"coll"`
}

// Partition represents a range of documents in a namespace, bounded by the _id field.
//
// A valid partition must have a non-nil lower bound (in its PartitionKey) and a non-nil upper bound.
type Partition struct {
	Key PartitionKey `bson:"_id"`
	Ns  *Namespace   `bson:"namespace"`

	// The upper index key bound for the partition.
	Upper any `bson:"upperBound"`

	// Set to true if the partition is for a capped collection. If so, this partition's
	// upper/lower bounds should be set to the minKey and maxKey of the collection.
	IsCapped bool `bson:"isCapped"`
}

// String returns a string representation of the partition.
func (p *Partition) String() string {
	return fmt.Sprintf(
		"{db: %s, coll: %s, collUUID: %s, mongosyncID: %s, lower: %s, upper: %s}",
		p.Ns.DB, p.Ns.Coll, p.Key.SourceUUID, p.Key.MongosyncID, p.GetLowerBoundString(), p.GetUpperBoundString())
}

// GetLowerBoundString returns the string representation of this partition's lower bound.
func (p *Partition) GetLowerBoundString() string {
	return p.getIndexKeyBoundString(p.Key.Lower)
}

// GetUpperBoundString returns the string representation of this partition's upper bound.
func (p *Partition) GetUpperBoundString() string {
	return p.getIndexKeyBoundString(p.Upper)
}

// getIndexKeyBoundString returns the string representation of the given index key bound.
func (p *Partition) getIndexKeyBoundString(bound any) string {
	switch b := bound.(type) {
	case bson.RawValue:
		return b.String()
	case primitive.MinKey:
		return `{"$minKey":1}`
	case primitive.MaxKey:
		return `{"$maxKey":1}`
	default:
		return fmt.Sprintf("%v", b)
	}
}

// lowerBoundFromCurrent takes the current value of a cursor and returns the value to save as
// the lower bound for the cursor. For capped collections, this is `nil`. For others it's the
// value of the `_id` field.
func (p *Partition) lowerBoundFromCurrent(current bson.Raw) (any, error) {
	if p.IsCapped {
		return nil, nil
	}

	if len(current) == 0 {
		return nil, nil
	}

	var doc bson.M
	err := bson.Unmarshal(current, &doc)
	if err != nil {
		return nil, errors.Wrap(err, "error unmarshaling raw document to bson.M")
	}

	if id, ok := doc["_id"]; ok {
		return id, nil
	}

	return nil, errors.New("could not find an '_id' element in the raw document")
}

func (p *Partition) GetAggregationStages(
	clusterInfo *util.ClusterInfo,
	filterAndPredicates bson.A,
	docKeyFields []string,
) mongo.Pipeline {
	findOpts := p.GetFindOptions(clusterInfo, filterAndPredicates)

	pl := mongo.Pipeline{}

	for _, opt := range findOpts {
		switch opt.Key {
		case "filter":
			pl = append(pl, bson.D{{"$match", opt.Value}})
		case "sort":
			pl = append(pl, bson.D{{"$sort", opt.Value}})
		default:
			panic("Unknown find opt: " + opt.Key)
		}
	}

	docKey := lo.Map(
		docKeyFields,
		func(fieldName string, _ int) string {
			return "$$ROOT." + fieldName
		},
	)

	pl = append(
		pl,
		bson.D{
			{"$replaceRoot", bson.D{
				{"newRoot", bson.D{
					{"docKey", docKey},
					{"hash", bson.D{
						{"$toHashedIndexKey", bson.D{
							{"$_internalKeyStringValue", bson.D{
								{"input", "$$ROOT"},
							}},
						}},
					}},
				}},
			}},
		},
	)

	return pl
}

// GetFindOptions returns only the options necessary to do a find on any given collection with this
// partition. It is intended to allow the same partitioning to be used on different collections
// (e.g. use the partitions on the source to read the destination for verification)
// If the passed-in buildinfo indicates a mongodb version < 5.0, type bracketing is not used.
// filterAndPredicates is a slice of filter criteria that's used to construct the "filter" field in the find option.
func (p *Partition) GetFindOptions(clusterInfo *util.ClusterInfo, filterAndPredicates bson.A) bson.D {
	if p == nil {
		if len(filterAndPredicates) > 0 {
			return bson.D{{"filter", bson.D{{"$and", filterAndPredicates}}}}
		}
		return bson.D{}
	}
	findOptions := bson.D{}
	if p.IsCapped {
		// For capped collections, sort the documents by their natural order. We deliberately
		// exclude the ID filter to ensure that documents are inserted in the correct order.
		sort := bson.E{"sort", bson.D{{"$natural", 1}}}
		findOptions = append(findOptions, sort)
	} else {
		// For non-capped collections, sort by _id to minimize the amount of time
		// that a given document spends cached in memory.
		findOptions = append(findOptions, bson.E{"sort", bson.D{{"_id", 1}}})

		// For non-capped collections, the cursor should use the ID filter and the _id index.
		// Get the bounded query filter from the partition to be used in the Find command.
		allowTypeBracketing := false
		if clusterInfo != nil {
			allowTypeBracketing = true

			if clusterInfo.VersionArray != nil {
				allowTypeBracketing = clusterInfo.VersionArray[0] < 5
			}
		}

		filterAndPredicates = slices.Clone(filterAndPredicates)

		if !allowTypeBracketing {
			filterAndPredicates = append(filterAndPredicates, p.filterWithNoTypeBracketing())
		} else {
			filterAndPredicates = append(filterAndPredicates, p.filterWithTypeBracketing())
		}

		hint := bson.E{"hint", bson.D{{"_id", 1}}}
		findOptions = append(findOptions, hint)
	}

	if len(filterAndPredicates) > 0 {
		findOptions = append(findOptions, bson.E{"filter", bson.D{{"$and", filterAndPredicates}}})
	}
	return findOptions
}

// filterWithNoTypeBracketing returns a range filter on _id to be used in a Find query for the
// partition.  This filter will properly handle mixed-type _ids, but on server versions
// < 5.0, will not use indexes and thus will be very slow
func (p *Partition) filterWithNoTypeBracketing() bson.D {
	// We use $expr to avoid type bracketing and allow comparison of different _id types,
	// and $literal to avoid MQL injection from an _id's value.
	return bson.D{{"$and", []bson.D{
		// All _id values >= lower bound.
		{{"$expr", bson.D{
			{"$gte", bson.A{
				"$_id",
				bson.D{{"$literal", p.Key.Lower}},
			}},
		}}},
		// All _id values <= upper bound.
		{{"$expr", bson.D{
			{"$lte", bson.A{
				"$_id",
				bson.D{{"$literal", p.Upper}},
			}},
		}}},
	}}}
}

// filterWithTypeBracketing returns a range filter on _id to be used in a Find query for the
// partition.  This filter will not properly handle mixed-type _ids -- if the upper and lower
// bounds are of different types (except minkey/maxkey), nothing will be returned.
func (p *Partition) filterWithTypeBracketing() bson.D {
	return bson.D{{"$and", []bson.D{
		// All _id values >= lower bound.
		{{"_id", bson.D{{"$gte", p.Key.Lower}}}},
		// All _id values <= upper bound.
		{{"_id", bson.D{{"$lte", p.Upper}}}},
	}}}
}
