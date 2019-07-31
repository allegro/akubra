package feeder

import (
	"database/sql"
	"database/sql/driver"
	"github.com/allegro/akubra/internal/brim/model"
	"testing"
	"time"

	wc "github.com/allegro/akubra/internal/akubra/watchdog/config"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/allegro/akubra/internal/akubra/config"
	"github.com/allegro/akubra/internal/akubra/watchdog"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const (
	logEntriesSelect = `SELECT\ \*\ FROM\ \"consistency_record\"\ WHERE\ .+`
)

type AnyTime struct{}

// Match satisfies sqlmock.Argument interface
func (a AnyTime) Match(v driver.Value) bool {
	_, ok := v.(time.Time)
	return ok
}

type dbClientFactoryMock struct {
	mock.Mock
}

func (factory *dbClientFactoryMock) CreateConnection(dbConfig map[string]string) (*gorm.DB, error) {
	args := factory.Called(dbConfig)
	dbClient := args.Get(0).(*gorm.DB)
	return dbClient, args.Error(1)
}

type walFilterMock struct {
	filterFunc func(walEntry *model.WALEntry) *model.WALTask
}

func (filter *walFilterMock) Filter(walEntriesChannel <-chan *model.WALEntry) <-chan *model.WALTask {
	walTaskChan := make(chan *model.WALTask)
	go func() {
		for entry := range walEntriesChannel {
			walTaskChan <- filter.filterFunc(entry)
		}
	}()
	return walTaskChan
}

type compaction struct {
	domain, objectID string
	objectVersion    int
	rowsAffected     int64
}

type failure struct {
	requestID string
	err       error
}

func TestShouldEmitASingleWALEntryForAGivenObjectInParticularDomain(t *testing.T) {

	watchdogProps := make(map[string]string)
	akubraConfig := config.YamlConfig{
		Watchdog: wc.WatchdogConfig{
			Type:  "sql",
			Props: watchdogProps,
		}}

	feederConfig := WALFeederConfig{NoRecordsSleepDuration: 10 * time.Second, MaxRecordsPerQuery: 10, FailureDelay: time.Minute * 5}

	records := []watchdog.SQLConsistencyRecord{
		{ObjectVersion: 1, RequestID: "1", ObjectID: "some/object1", Domain: "test1.qxlint", InsertedAt: time.Now().Add(-6 * time.Minute).UTC(), ExecutionDelay: (5 * time.Minute).String()},
		{ObjectVersion: 2, RequestID: "2", ObjectID: "some/object1", Domain: "test1.qxlint", InsertedAt: time.Now().Add(-12 * time.Minute).UTC(), ExecutionDelay: (5 * time.Minute).String()},
		{ObjectVersion: 3, RequestID: "3", ObjectID: "some/object1", Domain: "test1.qxlint", InsertedAt: time.Now().Add(-18 * time.Minute).UTC(), ExecutionDelay: (5 * time.Minute).String()},
		{ObjectVersion: 1, RequestID: "4", ObjectID: "some/object2", Domain: "test2.qxlint", InsertedAt: time.Now().Add(-6 * time.Minute).UTC(), ExecutionDelay: (5 * time.Minute).String()},
		{ObjectVersion: 2, RequestID: "5", ObjectID: "some/object2", Domain: "test2.qxlint", InsertedAt: time.Now().Add(-12 * time.Minute).UTC(), ExecutionDelay: (5 * time.Minute).String()},
		{ObjectVersion: 3, RequestID: "6", ObjectID: "some/object2", Domain: "test2.qxlint", InsertedAt: time.Now().Add(-16 * time.Minute).UTC(), ExecutionDelay: (5 * time.Minute).String()},
	}

	deleteParams := []compaction{
		{domain: records[0].Domain, objectID: records[0].ObjectID, objectVersion: records[0].ObjectVersion, rowsAffected: 3},
		{domain: records[3].Domain, objectID: records[3].ObjectID, objectVersion: records[3].ObjectVersion, rowsAffected: 3},
	}

	dbFactoryMock, db, _ := createDBFactoryMock(watchdogProps, records, deleteParams, []failure{}, t)
	defer db.Close()

	sqlWALFeeder, _ := NewSQLWALFeeder(&config.Config{YamlConfig: akubraConfig}, &feederConfig, dbFactoryMock)

	var emittedEntries []string
	entriesFeed := sqlWALFeeder.CreateFeed()

	for len(emittedEntries) < 2 {
		entry := <-entriesFeed
		_ = entry.RecordProcessedHook(entry.Record, nil)
		emittedEntries = append(emittedEntries, entry.Record.ObjectID)
	}

	assert.Len(t, emittedEntries, 2)
	assert.Len(t, entriesFeed, 0)
	assert.Contains(t, emittedEntries, "some/object1")
	assert.Contains(t, emittedEntries, "some/object2")
}

func TestShouldCommitATransactionEvenWhenSomeOfTheTasksHaveFailed(t *testing.T) {

	taskError := errors.New("Fail")
	watchdogProps := make(map[string]string)
	akubraConfig := config.YamlConfig{
		Watchdog: wc.WatchdogConfig{
			Type:  "sql",
			Props: watchdogProps,
		}}

	feederConfig := WALFeederConfig{NoRecordsSleepDuration: 10 * time.Second, MaxRecordsPerQuery: 10}

	records := []watchdog.SQLConsistencyRecord{
		{ObjectVersion: 1, RequestID: "1", ObjectID: "some/object1", Domain: "test1.qxlint", InsertedAt: time.Now().UTC(), ExecutionDelay: (5 * time.Minute).String()},
		{ObjectVersion: 1, RequestID: "2", ObjectID: "some/object2", Domain: "test2.qxlint", InsertedAt: time.Now().UTC(), ExecutionDelay: (5 * time.Minute).String()},
	}

	compactions := []compaction{{domain: records[0].Domain, objectID: records[0].ObjectID, objectVersion: records[0].ObjectVersion, rowsAffected: 1}}
	failures := []failure{{requestID: records[1].RequestID, err: taskError}}

	dbFactoryMock, db, _ := createDBFactoryMock(watchdogProps, records, compactions, failures, t)
	defer db.Close()

	sqlWALFeeder, _ := NewSQLWALFeeder(&config.Config{YamlConfig: akubraConfig}, &feederConfig, dbFactoryMock)

	var emittedEntries []string
	entriesFeed := sqlWALFeeder.CreateFeed()

	for len(emittedEntries) < 2 {
		entry := <-entriesFeed
		var err error
		if len(emittedEntries) == 1 {
			err = taskError
		}
		_ = entry.RecordProcessedHook(entry.Record, err)
		emittedEntries = append(emittedEntries, entry.Record.ObjectID)
	}

	assert.Len(t, emittedEntries, 2)
	assert.Len(t, entriesFeed, 0)
	assert.Contains(t, emittedEntries, "some/object1")
	assert.Contains(t, emittedEntries, "some/object2")
}

func createDBFactoryMock(watchdogProps map[string]string, records []watchdog.SQLConsistencyRecord, deleteParams []compaction, failures []failure, t *testing.T) (*dbClientFactoryMock, *sql.DB, sqlmock.Sqlmock) {
	dbFactoryMock := &dbClientFactoryMock{}
	db, dbMock, err := sqlmock.New()
	assert.NoError(t, err)
	gormDB, err := gorm.Open("postgres", db)
	assert.NoError(t, err)
	queryRows := sqlmock.NewRows([]string{"request_id", "object_id", "domain", "object_version", "execution_delay", "updated_at"})

	for idx := range records {
		queryRows.AddRow(records[idx].RequestID, records[idx].ObjectID, records[idx].Domain, records[idx].ObjectVersion, records[idx].ExecutionDelay, records[idx].UpdatedAt)
	}

	dbMock.ExpectBegin()

	dbMock.
		ExpectQuery(logEntriesSelect).
		WillReturnRows(queryRows)

	for idx := range deleteParams {
		dbMock.
			ExpectExec(`DELETE\ FROM\ \"consistency_record\"\ WHERE\ \(domain\ \=\ \$1\ AND\ object_id\ \=\ \$2\ AND\ object_version\ \<\=\ \$3\)`).
			WithArgs(deleteParams[idx].domain, deleteParams[idx].objectID, deleteParams[idx].objectVersion).
			WillReturnResult(sqlmock.NewResult(1, deleteParams[idx].rowsAffected))
	}

	for idx := range failures {

		dbMock.
			ExpectExec(`UPDATE\ \"consistency_record\"\ SET\ \"error\"\ \=\ .+\,\ \"updated_at\"\ \=\ .+\ WHERE\ \(request_id\ \=\ .+\)`).
			WithArgs(failures[idx].err.Error(), AnyTime{}, failures[idx].requestID).
			WillReturnResult(sqlmock.NewResult(1, 1))

		dbMock.
			ExpectQuery("UPDATE consistency_record SET execution_delay .+").
			WithArgs(failures[idx].requestID).
			WillReturnRows(dbMock.NewRows([]string{"x"}).AddRow("x")).
			RowsWillBeClosed()
	}

	dbMock.ExpectCommit()
	dbFactoryMock.On("CreateConnection", watchdogProps).Return(gormDB, nil)
	return dbFactoryMock, db, dbMock
}
