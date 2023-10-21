package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/google/uuid"
	"log"
	"os"
	"path"
	"sync/atomic"
)

//type ScreenEvent struct {
//	Timestamp  string `json:"timestamp"`
//	DeviceId   string `json:"device_id"`
//	DeviceName string `json:"device_name"`
//	ScreenName string `json:"screen_name"`
//	AppVersion string `json:"app_version"`
//}

type Job struct {
	data []byte
	opID uuid.UUID
}

type Result struct {
	err  error
	data []byte
	opID uuid.UUID
}

func worker(ctx context.Context, f func(ctx context.Context, job Job) Result, jch <-chan Job, rch chan<- Result) {
	for j := range jch {
		res := f(ctx, j)
		rch <- res
	}
}

func main() {
	//opID := "d0f5fa07-9d70-4250-ae34-ca054fe0d915"
	foldPath := "/Users/ekamaster/GolandProjects/app-event-processing/disk"
	fileName := "name"
	batchSiz := 100
	workSiz := 10

	fPath := path.Join(foldPath, fileName)

	jobs := make(chan Job, batchSiz)
	results := make(chan Result, batchSiz)

	for i := 1; i <= workSiz; i++ {
		go worker(context.TODO(), func(ctx context.Context, job Job) Result {
			res := Result{
				err:  nil,
				data: job.data,
				opID: job.opID,
			}
			return res
		}, jobs, results)
	}

	var siz int32

	go func() {
		f, err := os.Open(fPath)
		if err != nil {
			panic(err)
		}
		defer func(f *os.File) {
			err := f.Close()
			if err != nil {
				panic(err)
			}
		}(f)

		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			jobs <- Job{
				data: scanner.Bytes(),
				opID: uuid.New(),
			}
			atomic.AddInt32(&siz, 1)
		}
		close(jobs)

		if err := scanner.Err(); err != nil {
			log.Fatal(err)
		}
	}()

	for val := range results {
		fmt.Println(string(val.data))
		atomic.AddInt32(&siz, -1)
		if siz < 1 {
			close(results)
		}
	}
}
