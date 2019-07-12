package worker

import (
	"fmt"
	"net/http"
	"time"

	"github.com/allegro/akubra/internal/akubra/log"
	"github.com/allegro/akubra/internal/akubra/metrics"
	"github.com/allegro/akubra/internal/akubra/watchdog"
	"github.com/allegro/akubra/internal/brim/model"
	model2 "github.com/allegro/akubra/internal/brim/model"
	"github.com/allegro/akubra/internal/brim/s3"
	"github.com/allegro/akubra/internal/brim/util"
	"github.com/pkg/errors"
)

const oneHundredMB = 100000000

//WALWorker performs the migrations
type WALWorker interface {
	Process(walTasksChan <-chan *model.WALTask)
	SetMultiPartThresholdInBytes(numOfBytes int)
}

//TaskMigratorWALWorker uses TaskMigrator for migrations
type TaskMigratorWALWorker struct {
	semaphore              chan struct{}
	minMultiPartObjectSize int
}

func (walWorker *TaskMigratorWALWorker) SetMultiPartThresholdInBytes(numOfBytes int) {
	walWorker.minMultiPartObjectSize = oneHundredMB
}

//NewTaskMigratorWALWorker creates an instance of TaskMigratorWALWorker
func NewTaskMigratorWALWorker(maxConcurrentMigrations int) WALWorker {
	return &TaskMigratorWALWorker{
		semaphore:              make(chan struct{}, maxConcurrentMigrations),
		minMultiPartObjectSize: oneHundredMB}
}

//Process processes the channel of tasks and performs the migrations themselves
func (walWorker *TaskMigratorWALWorker) Process(walTasksChan <-chan *model.WALTask) {
	go func(walTasksChan <-chan *model.WALTask, semaphore chan struct{}) {
		for walTask := range walTasksChan {
			go func(task *model.WALTask) {

				record := task.WALEntry.Record
				if task.SourceClient == nil && len(task.DestinationsClients) == 0 {
					log.Debugf("No need to sync object '%s' in domain '%s'", record.ObjectID, record.Domain)
					_ = task.WALEntry.RecordProcessedHook(record, nil)
					return
				}

				err := walWorker.processTask(task)
				_ = task.WALEntry.RecordProcessedHook(record, err)

			}(walTask)
		}
	}(walTasksChan, walWorker.semaphore)
}

func (walWorker *TaskMigratorWALWorker) processTask(walTask *model.WALTask) error {
	dstEndpoints := make([]string, 0)
	for _, dstClient := range walTask.DestinationsClients {
		dstEndpoints = append(dstEndpoints, dstClient.S3Endpoint)
	}

	var err error
	since := time.Now()
	operation := "migration"
	switch walTask.WALEntry.Record.Method {
	case watchdog.PUT:
		log.Debugf("Performing migration of object %s in domain %s to version %s. Source %s -> destinations %s",
			walTask.WALEntry.Record.ObjectID, walTask.WALEntry.Record.Domain, walTask.WALEntry.Record.ObjectVersion,
			walTask.SourceClient.S3Endpoint, dstEndpoints)
		err = walWorker.performMigration(walTask)
	case watchdog.DELETE:
		operation = "delete"
		log.Debugf("Deleting object %s in domain %s from storages %s",
			walTask.WALEntry.Record.ObjectID, walTask.WALEntry.Record.Domain, dstEndpoints)
		err = walWorker.performDelete(walTask)
	default:
		return errors.New("unsupported method")
	}

	normalizedDomain := metrics.Clean(walTask.WALEntry.Record.Domain)
	if err == nil {
		metrics.UpdateSince(fmt.Sprintf("watchdog.%s.%s.success", operation, normalizedDomain), since)
	} else {
		metrics.UpdateSince(fmt.Sprintf("watchdog.%s.%s.failure", operation, normalizedDomain), since)
	}
	return err
}

func (walWorker *TaskMigratorWALWorker) performMigration(task *model.WALTask) error {
	bucketName, key, err := util.SplitKeyIntoBucketKey(task.WALEntry.Record.ObjectID)
	if err != nil {
		return err
	}

	srcBucket := task.SourceClient.Bucket(bucketName)

	resp, err := srcBucket.Head(key, http.Header{})
	if err != nil {
		return err
	}

	for _, dstClient := range task.DestinationsClients {
		migrator := s3.TaskMigrator{
			SrcS3Client: task.SourceClient,
			DstS3Client: dstClient,
			Task:        copyObjectTask(srcBucket.S3Endpoint, dstClient.S3Endpoint, bucketName, key),
			Multipart:   resp.ContentLength >= oneHundredMB,
		}

		walWorker.semaphore <- struct{}{}
		srcError, dstError := migrator.Run()
		<-walWorker.semaphore

		if srcError != nil {
			return srcError
		} else if dstError != nil {
			return dstError
		}
	}

	log.Debugf("Synchronization of object '%s' in domain '%s' successful",
		task.WALEntry.Record.ObjectID, task.WALEntry.Record.Domain)
	return nil
}
func copyObjectTask(srcEndpoint string, dstEndpoint string, bucket string, key string) s3.MigrationTaskData {
	return s3.NewMigrationTaskData("copy", model2.ACLCopyFromSource,
		srcEndpoint, dstEndpoint,
		bucket, key, bucket, key)
}

func (walWorker *TaskMigratorWALWorker) performDelete(task *model.WALTask) error {
	deletesPerformed := 0
	for _, client := range task.DestinationsClients {
		bucketName, key, err := util.SplitKeyIntoBucketKey(task.WALEntry.Record.ObjectID)
		if err != nil {
			return err
		}
		log.Debugf("Deleting object '%s/%s' from '%s'", bucketName, key, client.S3Endpoint)
		walWorker.semaphore <- struct{}{}
		bucket := client.Bucket(bucketName)
		err = bucket.Del(key)
		<-walWorker.semaphore
		if err != nil {
			return err
		}
		deletesPerformed++
		log.Printf("Deleted object '%s/%s' from '%s'", bucketName, key, client.S3Endpoint)
	}
	if deletesPerformed == 0 {
		log.Debugf("Nothing to do, object '%s' in domain '%s' is already deleted on all storages",
			task.WALEntry.Record.ObjectID, task.WALEntry.Record.Domain)
	}
	return nil
}
