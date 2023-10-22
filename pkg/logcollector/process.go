package logcollector

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
)

var (
	ErrMakeBatch  = errors.New("failed to create batches")
	ErrProcessing = errors.New("processing data failed")
)

func Process[T any](
	ctx context.Context,
	reader io.Reader,
	batchSize int,
	saveFunc func(ctx context.Context, batch []T) error,
) error {
	if batchSize < 1 {
		batchSize = 50
	}

	batches, err := split[T](reader, batchSize)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrMakeBatch, err)
	}

	var wg sync.WaitGroup
	results := make(chan error, len(batches))

	for _, batch := range batches {
		wg.Add(1)
		go func(ctx context.Context, b []T, r chan<- error) {
			defer wg.Done()
			err := saveFunc(ctx, b)
			r <- err
		}(ctx, batch, results)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var processError error
	for err := range results {
		if err != nil {
			processError = fmt.Errorf("%w: %w", ErrProcessing, err)
		}
	}

	return processError
}
