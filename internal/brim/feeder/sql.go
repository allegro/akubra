package feeder

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/allegro/akubra/internal/akubra/config"
	"github.com/allegro/akubra/internal/akubra/database"
	"github.com/allegro/akubra/internal/akubra/log"
	"github.com/allegro/akubra/internal/akubra/metrics"
	"github.com/allegro/akubra/internal/akubra/watchdog"
	"github.com/allegro/akubra/internal/brim/model"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	"github.com/pkg/errors"
)

// WALFeederConfig is a configuration for SQLWALFeeder
type WALFeederConfig struct {
	NoRecordsSleepDuration time.Duration `yaml:"NoRecordsSleepDuration"`
	MaxRecordsPerQuery     uint          `yaml:"MaxRecordsPerQuery"`
	FailureDelay           time.Duration `yaml:"FailureDelay"`
}

//SQLWALFeeder is an implementation of WALFeeder that creates a feed from a SQL DB
type SQLWALFeeder struct {
	WALFeeder
	db     *gorm.DB
	config *WALFeederConfig
}

//NewSQLWALFeeder construct an instance of SQLWALFeeder
func NewSQLWALFeeder(akubraConfig *config.Config,
	sqlFeederConfig *WALFeederConfig,
	dbClientFactory database.DBClientFactory) (WALFeeder, error) {
	if strings.ToLower(akubraConfig.Watchdog.Type) != "sql" {
		return nil, errors.New("Can't create SQL feeder if no SQL watchdog is defined")
	}
	db, err := dbClientFactory.CreateConnection(akubraConfig.Watchdog.Props)
	if err != nil {
		return nil, err
	}
	return &SQLWALFeeder{
		db:     db,
		config: sqlFeederConfig,
	}, nil
}

//CreateFeed streams WALEntries from the SQL DB
func (feeder *SQLWALFeeder) CreateFeed() <-chan *model.WALEntry {
	walEntriesChannel := make(chan *model.WALEntry, feeder.config.MaxRecordsPerQuery)
	go feeder.queryDB(walEntriesChannel)
	return walEntriesChannel
}

func (feeder *SQLWALFeeder) queryDB(walEntriesChannel chan *model.WALEntry) {
	for {

		log.Debugf("Querying database for at most %d consistency records", feeder.config.MaxRecordsPerQuery)

		consistencyRecords := make([]watchdog.SQLConsistencyRecord, feeder.config.MaxRecordsPerQuery)

		startTime := time.Now()
		tx := feeder.db.Begin()

		res := tx.
			Order("inserted_at desc").
			Set("gorm:query_option", "FOR UPDATE SKIP LOCKED").
			Where("updated_at + execution_delay < NOW()").
			Limit(feeder.config.MaxRecordsPerQuery).
			Find(&consistencyRecords)

		grouping := make(map[string]struct{})
		distinctRecords := make([]*watchdog.SQLConsistencyRecord, 0)

		for idx := range consistencyRecords {
			obj := fmt.Sprintf("%s%s", consistencyRecords[idx].Domain, consistencyRecords[idx].ObjectID)
			if _, seen := grouping[obj]; seen {
				continue
			}
			grouping[obj] = struct{}{}
			distinctRecords = append(distinctRecords, &consistencyRecords[idx])
		}

		if res.Error != nil {
			log.Printf("Failed on querying database for tasks: %s", res.Error)
			metrics.UpdateSince("watchdog.feeder.select.err", startTime)
			continue
		}

		log.Debugf("Gathered %d records from database in %f seconds", len(consistencyRecords), time.Now().Sub(startTime).Seconds())
		metrics.UpdateSince("watchdog.feeder.select.ok", startTime)

		wg := &sync.WaitGroup{}
		wg.Add(len(distinctRecords))
		go commitTransactionOnComplete(tx, wg)

		if len(distinctRecords) < 1 {
			log.Printf("No entries in the log. Waiting %.2f seconds", feeder.config.NoRecordsSleepDuration.Seconds())
			time.Sleep(feeder.config.NoRecordsSleepDuration)
		}

		for idx := range distinctRecords {
			consistencyRecord := mapSQLToRecord(distinctRecords[idx])
			walEntriesChannel <- &model.WALEntry{
				Record:              consistencyRecord,
				RecordProcessedHook: recordProcessedHook(tx, wg, feeder.config.FailureDelay, startTime),
			}
		}
		wg.Wait()
	}
}

func commitTransactionOnComplete(tx *gorm.DB, wg *sync.WaitGroup) {
	wg.Wait()
	if res := tx.Commit(); res.Error != nil {
		log.Printf("Failed to commit transaction after records processing: %s", res.Error)
		return
	}
}

func recordProcessedHook(tx *gorm.DB, wg *sync.WaitGroup, failureDelay time.Duration, taskStartTime time.Time) func(record *watchdog.ConsistencyRecord, err error) error {
	return func(record *watchdog.ConsistencyRecord, err error) error {
		defer wg.Done()

		if err != nil {
			metrics.UpdateSince("watchdog.worker.failure", taskStartTime)
			updateRecordError(tx, record, err)

			log.Printf("Error during processing of task for requestID = '%s': %s", record.RequestID, err)

			err := delayNextExecution(tx, record, failureDelay)
			if err != nil {
				log.Printf("Failed to extend execution delay for reqID = %s: %s", record.RequestID, err)
			}
			return err
		}

		metrics.UpdateSince("watchdog.worker.success", taskStartTime)
		return compactRecord(tx, record)
	}
}

func updateRecordError(tx *gorm.DB, record *watchdog.ConsistencyRecord, err error) {
	sqlRecord := &watchdog.SQLConsistencyRecord{RequestID: record.RequestID}
	tx.Model(sqlRecord).Where("request_id = ?", sqlRecord.RequestID).Update("error", err.Error())
}

func compactRecord(tx *gorm.DB, record *watchdog.ConsistencyRecord) error {
	queryStartTime := time.Now()

	syncedVersion, err := time.Parse(watchdog.VersionDateLayout, record.ObjectVersion)
	if err != nil {
		return err
	}

	deleteRes := tx.
		Where("domain = ? AND object_id = ? AND inserted_at <= ?",
			record.Domain, record.ObjectID, syncedVersion).
		Delete(watchdog.SQLConsistencyRecord{})

	if deleteRes.Error != nil {
		metrics.UpdateSince("watchdog.feeder.delete.err", queryStartTime)
		return fmt.Errorf("failed to remove records for obeject '%s' on domain '%s' older than '%s': %s",
			record.ObjectID, record.Domain, record.ObjectVersion, deleteRes.Error)
	}

	metrics.UpdateGauge("watchdog.feeder.compacted_records", deleteRes.RowsAffected)
	metrics.UpdateSince("watchdog.feeder.delete.ok", queryStartTime)

	log.Printf("Processed version '%s' of object '%s' on domain '%s'. Removing %d log entries",
		record.ObjectVersion, record.ObjectID, record.Domain, deleteRes.RowsAffected)

	return nil
}

func delayNextExecution(tx *gorm.DB, record *watchdog.ConsistencyRecord, delay time.Duration) error {
	insertionDate, err := time.Parse(watchdog.VersionDateLayout, record.ObjectVersion)
	if err != nil {
		return err
	}
	newExecutionDelay := time.Now().UTC().Sub(insertionDate) + delay
	sqlRecord := &watchdog.SQLConsistencyRecord{RequestID: record.RequestID}
	tx.Model(sqlRecord).Where("request_id = ?", sqlRecord.RequestID).Update("execution_delay", newExecutionDelay.String())
	return nil
}

func mapSQLToRecord(record *watchdog.SQLConsistencyRecord) *watchdog.ConsistencyRecord {
	return &watchdog.ConsistencyRecord{
		ObjectID:      record.ObjectID,
		Domain:        record.Domain,
		RequestID:     record.RequestID,
		Method:        watchdog.Method(record.Method),
		AccessKey:     record.AccessKey,
		ObjectVersion: record.InsertedAt.Format(watchdog.VersionDateLayout),
	}
}
