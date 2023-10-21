package logcollector

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
)

func split[T any](r io.Reader, batchSiz int) ([][]T, error) {
	scanner := bufio.NewScanner(r)

	batches := make([][]T, 0, 1)
	batches = append(batches, make([]T, 0, batchSiz))

	var rowNum int
	for scanner.Scan() {
		rowNum++
		idx := len(batches) - 1
		if len(batches[idx]) >= batchSiz {
			batches = append(batches, make([]T, 0, batchSiz))
			idx = len(batches) - 1
		}

		var row T
		err := json.Unmarshal(scanner.Bytes(), &row)
		if err != nil {
			return batches, fmt.Errorf("in row %v: %w", rowNum, err)
		}

		batches[idx] = append(batches[idx], row)
	}
	return batches, nil
}
