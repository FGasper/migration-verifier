package util

import (
	"context"
	"fmt"

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
	target *[]T,
) error {
	eventsRead := 0

	var newTarget []T

	for hasEventInBatch := true; hasEventInBatch; hasEventInBatch = reader.RemainingBatchLength() > 0 {
		gotEvent := reader.TryNext(ctx)

		if reader.Err() != nil {
			return errors.Wrap(reader.Err(), "failed to read cursor")
		}

		if !gotEvent {
			break
		}

		if newTarget == nil {
			newTarget = make([]T, reader.RemainingBatchLength()+1)
		}

		if err := reader.Decode(&newTarget[eventsRead]); err != nil {
			return errors.Wrapf(err, "failed to decode from cursor to %T", newTarget[0])
		}

		eventsRead++
	}

	fmt.Printf("\n========== got docs: %+v\n\n", newTarget)

	*target = newTarget

	return nil
}
