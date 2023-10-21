package logcollector

import (
	"context"
	"errors"
	"fmt"
	"io"
)

var (
	ErrMakeBatch  = errors.New("failed to create batches")
	ErrProcessing = errors.New("processing data failed")
)

func Process[T any](
	ctx context.Context,
	reader io.Reader,
	batchSiz int,
	saveFunc func(ctx context.Context, batch []T) error,
) error {
	if batchSiz < 1 {
		batchSiz = 50
	}

	batches, err := split[T](reader, batchSiz)
	if err != nil {
		return errors.Join(ErrMakeBatch, err)
	}

	results := make(chan error, len(batches))
	for _, batch := range batches {
		go func(ctx context.Context, b []T, r chan<- error) {
			err := saveFunc(ctx, b)
			r <- err
		}(ctx, batch, results)
	}

	var processError error
	for i := 0; i < len(batches); i++ {
		err = <-results
		if err != nil {
			processError = fmt.Errorf("%w: %w", ErrProcessing, err)
		}
	}

	return processError
}
