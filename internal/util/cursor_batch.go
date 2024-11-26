package util

import (
	"context"

	"github.com/pkg/errors"
)

type CursorLike interface {
	TryNext(context.Context) bool
	Err() error
	Decode(any) error
	RemainingBatchLength() int
}

func TryNextBatch[T any](
	ctx context.Context,
	reader CursorLike,
) ([]T, error) {
	eventsRead := 0
	var target []T

	for hasEventInBatch := true; hasEventInBatch; hasEventInBatch = reader.RemainingBatchLength() > 0 {
		gotEvent := reader.TryNext(ctx)

		if reader.Err() != nil {
			return nil, errors.Wrap(reader.Err(), "cursor iteration failed")
		}

		if !gotEvent {
			break
		}

		if target == nil {
			target = make([]T, reader.RemainingBatchLength()+1)
		}

		if err := reader.Decode(&(target[eventsRead])); err != nil {
			return nil, errors.Wrapf(err, "failed to decode to %T", target[0])
		}

		eventsRead++
	}

	return target, nil
}
