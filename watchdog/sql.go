package watchdog

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/allegro/akubra/log"
	"github.com/jinzhu/gorm"
)

type SQLWatchdogFactory struct {
	dialect                   string
	connectionStringFormat    string
	connectionStringArgsNames []string
}

type DatabaseWatchdog struct {
	dbConn *gorm.DB
}

type SQLConsistencyRecord struct {
	ObjectID      string
	Method        string
	Cluster       string
	AccessKey     string
	ExecutionDate time.Time
}

func CreateSQLWatchdogFactory(dialect, connStringFormat string, connStringArgsNames []string) ConsistencyWatchdogFactory {
	return &SQLWatchdogFactory{
		dialect: dialect,
		connectionStringFormat: connStringFormat,
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
	return nil, nil
}

func (watchdog *DatabaseWatchdog) Delete(marker *DeleteMarker) error {
	return nil

}
func (watchdog *DatabaseWatchdog) Update(record *ConsistencyRecord) error {
	return nil
}
