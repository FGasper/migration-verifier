package util

import (
	"context"

	"github.com/10gen/migration-verifier/internal/logger"
	"github.com/10gen/migration-verifier/mbson"
	"github.com/10gen/migration-verifier/mmongo"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type ClusterTopology string

type ClusterInfo struct {
	VersionArray              []int
	Topology                  ClusterTopology
	HasInternalKeyStringValue bool
}

const (
	TopologySharded ClusterTopology = "sharded"
	TopologyReplset ClusterTopology = "replset"
)

func GetClusterInfo(ctx context.Context, logger *logger.Logger, client *mongo.Client) (ClusterInfo, error) {
	va, err := getVersionArray(ctx, client)
	if err != nil {
		return ClusterInfo{}, errors.Wrap(err, "failed to fetch version array")
	}

	topology, err := getTopology(ctx, "hello", client)
	if err != nil {
		logger.Info().
			Err(err).
			Msgf("Failed to learn topology via %#q; falling back to %#q.", "hello", "isMaster")

		topology, err = getTopology(ctx, "isMaster", client)
		if err != nil {
			return ClusterInfo{}, errors.Wrapf(err, "failed to learn topology via %#q", "isMaster")
		}
	}

	var hasInternalKeyStringValue bool

	switch {
	case va[0] < 4:
		// Unsupported anyway, but hey.
		fallthrough
	case va[0] == 4 && va[1] < 4:
		hasInternalKeyStringValue = false
	default:
		hasInternalKeyStringValue, err = getSupportsInternalKeyStringValue(ctx, client)
		if err != nil {
			return ClusterInfo{}, err
		}
	}

	return ClusterInfo{
		VersionArray:              va,
		Topology:                  topology,
		HasInternalKeyStringValue: hasInternalKeyStringValue,
	}, nil
}

// NB: This doesn’t work for server 4.2, but that version
// never got $_internalKeyStringValue anyway.
func getSupportsInternalKeyStringValue(ctx context.Context, client *mongo.Client) (bool, error) {
	operator := "$_internalKeyStringValue"

	_, err := client.Database("x").Collection("x").Aggregate(
		ctx,
		mongo.Pipeline{
			{{"$addFields", bson.D{
				{"x", bson.D{
					{operator, bson.D{
						{"input", ""},
					}},
				}},
			}}},
			{{"$limit", 1}},
		},
	)

	if mmongo.ErrorHasCode(err, InvalidPipelineOperatorErrCode) {
		return false, nil
	}

	if errors.Is(err, mongo.ErrNoDocuments) {
		err = nil
	}

	if err != nil {
		return false, errors.Wrapf(err, "failed to determine support for %#q operator", operator)
	}

	return true, nil
}

func getVersionArray(ctx context.Context, client *mongo.Client) ([]int, error) {
	commandResult := client.Database("admin").RunCommand(ctx, bson.D{{"buildinfo", 1}})

	rawResp, err := commandResult.Raw()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to run %#q", "buildinfo")
	}

	var va []int
	_, err = mbson.RawLookup(rawResp, &va, "versionArray")
	if err != nil {
		return nil, errors.Wrap(err, "failed to decode build info version array")
	}

	return va, nil
}

func getTopology(ctx context.Context, cmdName string, client *mongo.Client) (ClusterTopology, error) {

	resp := client.Database("admin").RunCommand(
		ctx,
		bson.D{{cmdName, 1}},
	)

	raw, err := resp.Raw()
	if err != nil {
		return "", errors.Wrapf(err, "failed learn topology via %#q", cmdName)
	}

	hasMsg, err := mbson.RawContains(raw, "msg")
	if err != nil {
		return "", errors.Wrapf(err, "failed to check for %#q in %#q response (%v)", "msg", cmdName, raw)
	}

	return lo.Ternary(hasMsg, TopologySharded, TopologyReplset), nil
}
