package watchdog

import (
	"errors"
	"fmt"
	"github.com/allegro/akubra/internal/akubra/watchdog/config"
	"strings"
	"time"

	"github.com/allegro/akubra/internal/akubra/database"
	"github.com/allegro/akubra/internal/akubra/log"
	"github.com/allegro/akubra/internal/akubra/metrics"
	"github.com/jinzhu/gorm"
)

const (
	selectNow                        = "SELECT NOW()"
	watchdogTable                    = "consistency_record"
	markersInsertedEalier            = "domain = ? AND object_id = ? AND inserted_at <= ?"
	updateRecordExecutionTimeByReqID = "UPDATE consistency_record " +
		"SET execution_delay = ?" +
		"WHERE request_id = ?"
	//Reader turns on reader config generation
	Reader = true
	//Writer turn on writer config generation
	Writer = false
)

// SQLWatchdogFactory creates instances of SQLWatchdog
type SQLWatchdogFactory struct {
	dbClientFactory database.DBClientFactory
}

// SQLWatchdog is a type of ConsistencyWatchdog that uses a SQL database
type SQLWatchdog struct {
	dbConn            *gorm.DB
	versionHeaderName string
}

// ErrDataBase indicates a database errors
var ErrDataBase = errors.New("database error")

// SQLConsistencyRecord is a SQL representation of ConsistencyRecord
type SQLConsistencyRecord struct {
	InsertedAt     time.Time `gorm:"column:inserted_at;default:NOW()"`
	UpdatedAt      time.Time `gorm:"column:updated_at"`
	ObjectID       string    `gorm:"column:object_id"`
	Method         string    `gorm:"column:method"`
	Domain         string    `gorm:"column:domain"`
	AccessKey      string    `gorm:"column:access_key"`
	ExecutionDelay string    `gorm:"column:execution_delay"`
	RequestID      string    `gorm:"column:request_id"`
	Error          string    `gorm:"column:error"`
}

//TableName provides the table name for consistency_record
func (SQLConsistencyRecord) TableName() string {
	return "consistency_record"
}

var sqlParams = map[string]string{
	"user":            "user",
	"password":        "password",
	"dbname":          "dbname",
	"host":            "host",
	"port":            "port",
	"conntimeout":     "conntimeout",
	"connmaxlifetime": "connmaxlifetime",
}

var configurableSQLParams = map[string]string{
	"%sopenconns": "maxopenconns",
	"%sidleconns": "maxidleconns",
}

// CreateSQLWatchdogFactory creates instances of SQLWatchdogFactory
func CreateSQLWatchdogFactory(dbClientFactory *database.GORMDBClientFactory) ConsistencyWatchdogFactory {
	return &SQLWatchdogFactory{dbClientFactory: dbClientFactory}
}

// CreateSQL creates ConsistencyWatchdog and ConsistencyRecordFactory that make use of a SQL database
func CreateSQL(dialect, connStringFormat string, params []string, watchdogConfig *config.WatchdogConfig) (ConsistencyWatchdog, error) {
	sqlWatchdogFactory := CreateSQLWatchdogFactory(database.NewDBClientFactory(dialect, connStringFormat, params))
	watchdog, err := sqlWatchdogFactory.CreateWatchdogInstance(watchdogConfig)
	if err != nil {
		return nil, err
	}
	return watchdog, nil
}

// CreateWatchdogInstance creates instances of SQLWatchdog
func (factory *SQLWatchdogFactory) CreateWatchdogInstance(config *config.WatchdogConfig) (ConsistencyWatchdog, error) {
	if strings.ToLower(config.Type) != "sql" {
		return nil, fmt.Errorf("SQLWatchdogFactory can't instantiate watchdog of type '%s'", config.Type)
	}
	dbConfig := CreateWatchdogSQLClientProps(config, Writer)
	db, err := factory.dbClientFactory.CreateConnection(dbConfig)
	if err != nil {
		return nil, err
	}

	log.Printf("SQLWatchdog watcher setup successful")

	return &SQLWatchdog{dbConn: db, versionHeaderName: config.ObjectVersionHeaderName}, nil
}

// Insert inserts to SQL db
func (watchdog *SQLWatchdog) Insert(record *ConsistencyRecord) (*DeleteMarker, error) {
	log.Debugf("Inserting consistency record for object '%s'", record.ObjectID)
	sqlRecord := createSQLRecord(record)

	queryStartTime := time.Now()
	insertResult := watchdog.dbConn.Table(watchdogTable).Create(sqlRecord)

	if insertResult.Error != nil {
		metrics.UpdateSince("watchdog.insert.err", queryStartTime)
		log.Printf("Failed to insert consistency record for object '%s'", sqlRecord.ObjectID)
		return nil, ErrDataBase
	}

	metrics.UpdateSince("watchdog.insert.ok", queryStartTime)

	insertedRecord, _ := insertResult.Value.(*SQLConsistencyRecord)
	insertedRecord.InsertedAt = insertedRecord.InsertedAt.UTC()
	log.Debugf("Successfully inserted consistency record for object '%s'", record.ObjectID)
	record.ObjectVersion = insertedRecord.InsertedAt.Format(VersionDateLayout)
	return createDeleteMarkerFor(insertedRecord), nil
}

//InsertWithRequestID inserts a record with custom ID
func (watchdog *SQLWatchdog) InsertWithRequestID(requestID string, record *ConsistencyRecord) (*DeleteMarker, error) {
	record.RequestID = requestID
	return watchdog.Insert(record)
}

// Delete deletes from SQL db
func (watchdog *SQLWatchdog) Delete(marker *DeleteMarker) error {

	queryStartTime := time.Now()
	deleteResult := watchdog.
		dbConn.
		Table(watchdogTable).
		Where(markersInsertedEalier, marker.domain, marker.objectID, marker.insertionDate).
		Delete(&ConsistencyRecord{})

	if deleteResult.Error != nil {
		metrics.UpdateSince("watchdog.delete.err", queryStartTime)
		log.Debugf("Failed to delete records for object '%s' older than %s: %s", marker.objectID, marker.insertionDate, deleteResult.Error)
		return ErrDataBase
	}

	metrics.UpdateSince("watchdog.delete.ok", queryStartTime)

	log.Debugf("Successfully deleted records for object '%s' older than %s", marker.objectID, marker.insertionDate.Format(time.RFC3339))
	return nil
}

// UpdateExecutionDelay updates execution time of a record in SQL db
func (watchdog *SQLWatchdog) UpdateExecutionDelay(delta *ExecutionDelay) error {

	queryStartTime := time.Now()
	updateErr := watchdog.
		dbConn.
		Exec(updateRecordExecutionTimeByReqID, fmt.Sprintf("%d minutes", uint64(delta.Delay.Minutes())), delta.RequestID).
		Error

	if updateErr != nil {
		metrics.UpdateSince("watchdog.update.err", queryStartTime)
		log.Printf("Failed to update record for reqId '%s'", delta.RequestID)
	}

	log.Debugf("Successfully updated record for reqId '%s'", delta.RequestID)
	return nil
}

// SupplyRecordWithVersion queries database for NOW and sets it as object's version
func (watchdog *SQLWatchdog) SupplyRecordWithVersion(record *ConsistencyRecord) error {
	rows, err := watchdog.
		dbConn.
		Raw(selectNow).
		Rows()

	if err != nil {
		log.Debugf("Failed to supply object with version: %s", err)
		return ErrDataBase
	}
	if !rows.Next() {
		log.Debugf("Empty response from database")
		return ErrDataBase
	}

	var objectVersion time.Time

	err = rows.Scan(&objectVersion)
	if err != nil {
		return err
	}

	record.ObjectVersion = objectVersion.String()
	return nil
}

//GetVersionHeaderName returns the name of the HTTP header that should hold to object's verison
func (watchdog *SQLWatchdog) GetVersionHeaderName() string {
	return watchdog.versionHeaderName
}

func createDeleteMarkerFor(record *SQLConsistencyRecord) *DeleteMarker {
	return &DeleteMarker{
		objectID:      record.ObjectID,
		domain:        record.Domain,
		insertionDate: record.InsertedAt,
	}
}

func createSQLRecord(record *ConsistencyRecord) *SQLConsistencyRecord {
	return &SQLConsistencyRecord{
		RequestID:      record.RequestID,
		ObjectID:       record.ObjectID,
		Method:         string(record.Method),
		ExecutionDelay: fmt.Sprintf("%d minutes", uint64(record.ExecutionDelay.Minutes())),
		AccessKey:      record.AccessKey,
		Domain:         record.Domain,
	}
}

//CreateWatchdogSQLClientProps creates watchdog reader/writer config
func CreateWatchdogSQLClientProps(watchdogConfig *config.WatchdogConfig, readerConfig bool) map[string]string {
	propPrefix := "writer"
	if readerConfig {
		propPrefix = "reader"
	}
	dbConfig := make(map[string]string)
	for watchdogConfigPropName, dbConnPropName := range sqlParams {
		dbConfig[dbConnPropName] = watchdogConfig.Props[watchdogConfigPropName]
	}
	for watchdogConfigPropName, dbConnPropName := range configurableSQLParams {
		dbConfig[dbConnPropName] = watchdogConfig.Props[fmt.Sprintf(watchdogConfigPropName, propPrefix)]
	}
	return dbConfig
}
