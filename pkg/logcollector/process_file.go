package logcollector

import (
	"context"
	"errors"
	"os"
)

var ErrOpenFile = errors.New("open file failed")

func ProcessFile[T any](
	ctx context.Context,
	path string,
	batchSiz int,
	saveFunc func(ctx context.Context, batch []T) error,
) error {
	f, err := os.Open(path)
	if err != nil {
		return errors.Join(ErrOpenFile, err)
	}

	return Process(ctx, f, batchSiz, saveFunc)
}
