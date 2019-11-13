package watchdog

import (
	"database/sql"
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
	insertNew                        = "INSERT INTO consistency_record (request_id, object_id, domain, access_key, execution_delay, method) VALUES (?, ?, ?, ?, ?, ?) RETURNING object_version"
	insertNewWithObjectVersion       = "INSERT INTO consistency_record (object_version, request_id, object_id, domain, access_key, execution_delay, method) VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING object_version"
	selectNow                        = "SELECT CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP at time zone 'utc') * 10^6 AS BIGINT)"
	deleteMarkersInsertedEalier      = "DELETE FROM consistency_record WHERE domain = ? AND object_id = ? AND object_version <= ?"
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
	ObjectVersion  int       `gorm:"column:object_version;default:EXTRACT(EPOCH FROM CURRENT_TIMESTAMP at time zone 'utc') * 10^6"`
	InsertedAt     time.Time `gorm:"column:inserted_at"`
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
	log.Debugf("[watchdog] INSERT reqID %s, objID %s, domain %s ", record.RequestID, record.ObjectID, record.Domain)

	queryStartTime := time.Now()

	var rows *sql.Rows
	var err error
	if record.ObjectVersion > 0 {

		rows, err = watchdog.
			dbConn.
			Raw(insertNewWithObjectVersion, record.ObjectVersion, record.RequestID, record.ObjectID, record.Domain, record.AccessKey, record.ExecutionDelay.String(), record.Method).
			Rows()

	} else {

		rows, err = watchdog.
			dbConn.
			Raw(insertNew, record.RequestID, record.ObjectID, record.Domain, record.AccessKey, record.ExecutionDelay.String(), record.Method).
			Rows()

	}

	if err != nil {
		metrics.UpdateSince("watchdog.insert.err", queryStartTime)
		log.Debugf("[watchdog] INSERT FAIL reqID %s, objID %s, domain %s: %s", record.RequestID, record.ObjectID, record.Domain, err.Error())
		return nil, ErrDataBase
	}

	defer rows.Close()
	metrics.UpdateSince("watchdog.insert.ok", queryStartTime)

	if !rows.Next() {
		return nil, ErrDataBase
	}

	var objVersion int
	err = rows.Scan(&objVersion)
	if err != nil {
		return nil, ErrDataBase
	}

	log.Debugf("[watchdog] INSERT OK reqID %s, objID %s, domain %s, version %d", record.RequestID, record.ObjectID, record.Domain, objVersion)
	record.ObjectVersion = objVersion
	return &DeleteMarker{
		objectID:      record.ObjectID,
		domain:        record.Domain,
		objectVersion: objVersion,
	}, nil
}

//InsertWithRequestID inserts a record with custom ID
func (watchdog *SQLWatchdog) InsertWithRequestID(requestID string, record *ConsistencyRecord) (*DeleteMarker, error) {
	record.RequestID = requestID
	return watchdog.Insert(record)
}	

// Delete deletes from SQL db
func (watchdog *SQLWatchdog) Delete(marker *DeleteMarker) error {
	log.Debugf("[watchdog] DELETE objID %s, version %d", marker.objectID, marker.objectVersion)
	queryStartTime := time.Now()
	rows, err := watchdog.
		dbConn.
		Raw(deleteMarkersInsertedEalier, marker.domain, marker.objectID, marker.objectVersion).
		Rows()

	defer func(){
		_ = rows.Close()
	}()

	if err != nil {
		metrics.UpdateSince("watchdog.delete.err", queryStartTime)
		log.Debugf("[watchdog] DELETE FAIL objID %s, version <= %d: %s", marker.objectID, marker.objectVersion, err)
		return ErrDataBase
	}

	metrics.UpdateSince("watchdog.delete.ok", queryStartTime)

	log.Debugf("[watchdog] DELETE OK objID, version <= %d", marker.objectID, marker.objectVersion)
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
		log.Printf("[watchdog] UPDATE EXEC FAIL delay reqID: %s", delta.RequestID, updateErr.Error())
	}

	log.Printf("[watchdog] UPDATE EXEC OK delay reqID ", delta.RequestID)
	return nil
}

// SupplyRecordWithVersion queries database for NOW and sets it as object's version
func (watchdog *SQLWatchdog) SupplyRecordWithVersion(record *ConsistencyRecord) error {
	rows, err := watchdog.
		dbConn.
		Raw(selectNow).
		Rows()

	if err != nil {
		log.Debugf("[watchdog] VERSION SUPPLY ERR reqID %s: %s", record.RequestID, err.Error())
		return ErrDataBase
	}

	defer rows.Close()

	if !rows.Next() {
		log.Debugf("[watchdog] VERSION SUPPLY FAIL %s: Empty response from database", record.RequestID)
		return ErrDataBase
	}

	var objectVersion int

	err = rows.Scan(&objectVersion)
	if err != nil {
		return err
	}

	record.ObjectVersion = objectVersion
	log.Debugf("[watchdog] VERSION SUPPLY OK reqID %s", record.RequestID)
	return nil
}

//GetVersionHeaderName returns the name of the HTTP header that should hold to object's verison
func (watchdog *SQLWatchdog) GetVersionHeaderName() string {
	return watchdog.versionHeaderName
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
