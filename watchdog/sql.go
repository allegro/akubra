package watchdog

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/allegro/akubra/log"
	"github.com/jinzhu/gorm"
)

const (
	selectNow                        = "SELECT NOW()"
	watchdogTable                    = "consistency_record"
	markersInsertedEalier            = "domain = ? AND object_id = ? AND inserted_at <= ?"
	updateRecordExecutionTimeByReqId = "UPDATE consistency_record " +
		"SET execution_delay = ?" +
		"WHERE request_id = ?"
)

// SQLWatchdogFactory creates instances of SQLWatchdog
type SQLWatchdogFactory struct {
	dialect                   string
	connectionStringFormat    string
	connectionStringArgsNames []string
}

// SQLWatchdog is a type of ConsistencyWatchdog that uses a SQL database
type SQLWatchdog struct {
	dbConn                  *gorm.DB
	objectVersionHeaderName string
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
}

// CreateSQLWatchdogFactory creates instances of SQLWatchdogFactory
func CreateSQLWatchdogFactory(dialect, connStringFormat string, connStringArgsNames []string) ConsistencyWatchdogFactory {
	return &SQLWatchdogFactory{
		dialect:                   dialect,
		connectionStringFormat:    connStringFormat,
		connectionStringArgsNames: connStringArgsNames,
	}
}

// CreateWatchdogInstance creates instances of SQLWatchdog
func (factory *SQLWatchdogFactory) CreateWatchdogInstance(config *Config) (ConsistencyWatchdog, error) {
	if strings.ToLower(config.Type) != "sql" {
		return nil, fmt.Errorf("SQLWatchdogFactory can't instantiate watchdog of type '%s'", config.Type)
	}

	connMaxLifetime, err := time.ParseDuration(config.Props["connmaxlifetime"])
	if err != nil {
		return nil, fmt.Errorf("failed to create SQLWatcher, couldn't parse 'connmaxlifetime': %s", err.Error())
	}

	maxOpenConns, err := strconv.Atoi(config.Props["maxopenconns"])
	if err != nil {
		return nil, fmt.Errorf("failed to create SQLWatcher, couldn't parse 'maxopenconns': %s", err.Error())
	}

	maxIdleConns, err := strconv.Atoi(config.Props["maxidleconns"])
	if err != nil {
		return nil, fmt.Errorf("failed to create SQLWatcher, couldn't parse 'maxidleconns': %s", err.Error())
	}

	connString, err := factory.createConnString(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create SQLWatcher, couldn't prepare connection string: %s", err.Error())
	}

	db, err := gorm.Open(factory.dialect, connString)
	if err != nil {
		return nil, fmt.Errorf("failed to create SQLWatcher, couldn't connect to db: %s", err.Error())
	}

	db.DB().SetConnMaxLifetime(connMaxLifetime)
	db.DB().SetMaxOpenConns(maxOpenConns)
	db.DB().SetMaxIdleConns(maxIdleConns)

	log.Printf("SQLWatchdog '%s' watcher setup successful", factory.dialect)

	return &SQLWatchdog{
		dbConn:                  db,
		objectVersionHeaderName: config.ObjectVersionHeaderName}, nil
}

func (factory *SQLWatchdogFactory) createConnString(config *Config) (string, error) {
	connString := factory.connectionStringFormat
	for _, argName := range factory.connectionStringArgsNames {
		if argValue, isArgProvided := config.Props[argName]; isArgProvided {
			connString = strings.Replace(connString, fmt.Sprintf(":%s:", argName), argValue, 1)
		} else {
			return "", fmt.Errorf("conn argument '%s' missing", argName)
		}
	}
	return connString, nil
}

// Insert inserts to SQL db
func (watchdog *SQLWatchdog) Insert(record *ConsistencyRecord) (*DeleteMarker, error) {
	log.Debugf("Inserting consistency record for object '%s'", record.objectID)
	sqlRecord := createSQLRecord(record)
	insertResult := watchdog.dbConn.Table(watchdogTable).Create(sqlRecord)

	if insertResult.Error != nil {
		log.Printf("Failed to insert consistency record for object '%s'", sqlRecord.ObjectID)
		return nil, ErrDataBase
	}

	insertedRecord, _ := insertResult.Value.(*SQLConsistencyRecord)
	log.Debugf("Successfully inserted consistency record for object '%s'", record.objectID)

	return createDeleteMarkerFor(insertedRecord), nil
}

func (watchdog *SQLWatchdog) InsertWithRequestID(requestID string, record *ConsistencyRecord) (*DeleteMarker, error) {
	record.RequestID = requestID
	return watchdog.Insert(record)
}

// Delete deletes from SQL db
func (watchdog *SQLWatchdog) Delete(marker *DeleteMarker) error {
	deleteResult := watchdog.
		dbConn.
		Table(watchdogTable).
		Where(markersInsertedEalier, marker.domain, marker.objectID, marker.insertionDate).
		Delete(&ConsistencyRecord{})

	if deleteResult.Error != nil {
		log.Debugf("Failed to delete records for object '%s' older than %s: %s", marker.objectID, marker.insertionDate, deleteResult.Error)
		return ErrDataBase
	}

	if deleteResult.RowsAffected < 1 {
		return fmt.Errorf("no records for object '%s' older than %s were deleted", marker.objectID, marker.insertionDate)
	}

	log.Debugf("Successfully deleted records for object '%s' older than %s", marker.objectID, marker.insertionDate.Format(time.RFC3339))
	return nil
}

// UpdateExecutionDelay updates execution time of a record in SQL db
func (watchdog *SQLWatchdog) UpdateExecutionDelay(delta *ExecutionDelay) error {
	updateErr := watchdog.
		dbConn.
		Exec(updateRecordExecutionTimeByReqId, fmt.Sprintf("%d minutes", uint64(delta.Delay.Minutes())), delta.RequestID).
		Error

	if updateErr != nil {
		log.Printf("Failed to update record for reqId '%s' on domain '%s'", delta.RequestID)
	}

	log.Debugf("Successfully updated record for reqId '%s' on domain '%s", delta.RequestID)
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

	record.objectVersion = objectVersion.String()
	return nil
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
		ObjectID:       record.objectID,
		Method:         string(record.method),
		ExecutionDelay: fmt.Sprintf("%d minutes", uint64(record.ExecutionDelay.Minutes())),
		AccessKey:      record.accessKey,
		Domain:         record.domain,
	}
}
