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
	watchdogTable         = "consistency_record"
	markersInsertedEalier = "cluster_name = ? AND object_id = ? AND inserted_at <= ?"
	updateRecordExecutionTime = "UPDATE consistency_record " +
								"SET execution_date = execution_date + (? ||' seconds')::interval, updated_at = NOW() " +
								"WHERE request_id = (SELECT request_id FROM consistency_record WHERE cluster_name = ? AND object_id = ? ORDER BY inserted_at LIMIT 1)"
)

type SQLWatchdogFactory struct {
	dialect                   string
	connectionStringFormat    string
	connectionStringArgsNames []string
}

type DatabaseWatchdog struct {
	dbConn *gorm.DB
}

var DatabaseError = errors.New("database error")

type SQLConsistencyRecord struct {
	InsertedAt    time.Time `gorm:"-"`
	UpdatedAt     time.Time `gorm:"-"`
	ObjectID      string    `gorm:"column:object_id"`
	Method        string    `gorm:"column:method"`
	Cluster       string    `gorm:"column:cluster_name"`
	AccessKey     string    `gorm:"column:access_key"`
	ExecutionDate time.Time `gorm:"column:execution_date"`
	RequestId     string    `gorm:"column:request_id"`
}

func CreateSQLWatchdogFactory(dialect, connStringFormat string, connStringArgsNames []string) ConsistencyWatchdogFactory {
	return &SQLWatchdogFactory{
		dialect:                   dialect,
		connectionStringFormat:    connStringFormat,
		connectionStringArgsNames: connStringArgsNames,
	}
}

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

	return &DatabaseWatchdog{dbConn: db}, nil
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

func (watchdog *DatabaseWatchdog) Insert(record *ConsistencyRecord) (*DeleteMarker, error) {
	log.Debugf("Inserting consistency record for object '%s'", record.objectID)
	sqlRecord := createSQLRecord(record)
	insertResult := watchdog.dbConn.Table(watchdogTable).Create(sqlRecord)

	if insertResult.Error != nil {
		log.Printf("Failed to insert consistency record for object '%s'", sqlRecord.ObjectID)
		return nil, DatabaseError
	}

	insertedRecord, _ := insertResult.Value.(*SQLConsistencyRecord)
	log.Debugf("Successfully inserted consistency record for object '%s'", record.objectID)

	return createDeleteMarkerFor(insertedRecord), nil
}

func (watchdog *DatabaseWatchdog) Delete(marker *DeleteMarker) error {
	deleteResult := watchdog.
		dbConn.
		Table(watchdogTable).
		Where(markersInsertedEalier, marker.cluster, marker.objectID, marker.insertionDate).
		Delete(&ConsistencyRecord{})

	if deleteResult.Error != nil {
		log.Debugf("Failed to delete records for object '%s' older than %s: %s", marker.objectID, marker.insertionDate, deleteResult.Error)
		return DatabaseError
	}

	log.Debugf("Successfully deleted records for object '%s' older than %s", marker.objectID, marker.insertionDate.Format(time.RFC3339))
	return nil
}

func (watchdog *DatabaseWatchdog) UpdateExecutionTime(delta *ExecutionTimeDelta) error {
	updateErr := watchdog.
		dbConn.
		Exec(updateRecordExecutionTime, delta.Delta, delta.ClusterName, delta.ObjectId).
		Error

	if updateErr != nil {
		log.Printf("Failed to update record for obj '%s' on cluster '%s'", delta.ObjectId, delta.ClusterName)
	}

	log.Debugf("Successfully updated record for obj '%s' on cluster '%s", delta.ObjectId, delta.ClusterName)
	return nil
}

func createDeleteMarkerFor(record *SQLConsistencyRecord) *DeleteMarker {
	return &DeleteMarker{
		objectID:      record.ObjectID,
		cluster:       record.Cluster,
		insertionDate: record.InsertedAt,
	}
}
func createSQLRecord(record *ConsistencyRecord) *SQLConsistencyRecord {
	return &SQLConsistencyRecord{
		RequestId:     record.requestId,
		ObjectID:      record.objectID,
		Method:        string(record.method),
		ExecutionDate: record.ExecutionDate,
		AccessKey:     record.accessKey,
		Cluster:       record.cluster,
	}
}