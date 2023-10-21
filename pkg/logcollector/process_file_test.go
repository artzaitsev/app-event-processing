package logcollector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"testing"
)

func TestProcessFile(t *testing.T) {
	type args[T any] struct {
		ctx      context.Context
		path     string
		batchSiz int
		saveFunc func(ctx context.Context, batch []T) error
	}
	type testCase[T any] struct {
		name    string
		args    args[T]
		wantErr bool
	}

	dir, _ := os.Getwd()
	tests := []testCase[Row]{
		{
			name: "process the file",
			args: args[Row]{
				ctx:      context.TODO(),
				path:     path.Join(dir, "testdata", "log"),
				batchSiz: 1,
				saveFunc: func(ctx context.Context, batch []Row) error {
					return nil
				},
			},
			wantErr: false,
		},
		{
			name: "broken log, unmarshall error",
			args: args[Row]{
				ctx:      context.TODO(),
				path:     path.Join(dir, "testdata", "log_broken"),
				batchSiz: 1,
				saveFunc: func(ctx context.Context, batch []Row) error {
					for _, data := range batch {
						d, _ := json.Marshal(data)
						fmt.Println(string(d))
					}

					return nil
				},
			},
			wantErr: true,
		},
		{
			name: "invalid file path",
			args: args[Row]{
				ctx:      context.TODO(),
				path:     path.Join(dir, "testdata", "loggs"),
				batchSiz: 1,
				saveFunc: func(ctx context.Context, batch []Row) error {
					for _, data := range batch {
						d, _ := json.Marshal(data)
						fmt.Println(string(d))
					}

					return nil
				},
			},
			wantErr: true,
		},
		{
			name: "error from saveFunc",
			args: args[Row]{
				ctx:      context.TODO(),
				path:     path.Join(dir, "testdata", "log"),
				batchSiz: 1,
				saveFunc: func(ctx context.Context, batch []Row) error {
					return errors.New("error")
				},
			},
			wantErr: true,
		},
		{
			name: "process the file with default batch size",
			args: args[Row]{
				ctx:      context.TODO(),
				path:     path.Join(dir, "testdata", "log"),
				batchSiz: 0,
				saveFunc: func(ctx context.Context, batch []Row) error {
					return nil
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ProcessFile(tt.args.ctx, tt.args.path, tt.args.batchSiz, tt.args.saveFunc); (err != nil) != tt.wantErr {
				t.Errorf("ProcessFile() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

type Row struct {
	Timestamp  string `json:"timestamp"`
	DeviceID   string `json:"device_id"`
	DeviceName string `json:"device_name"`
	ScreenName string `json:"screen_name"`
	AppVersion string `json:"app_version"`
}
