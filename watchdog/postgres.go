package watchdog

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/allegro/akubra/log"
)

const (
	connectionStringFormat = "dbname=%s user=%s password=%s host=%s port=%s connect_timeout=%s"
)

type PostgresWatchdogFactory struct {}

type PostgresWatchdog struct {
	dbConn *sql.DB
}

func (factory *PostgresWatchdogFactory) CreateWatchdogInstance(config *Config) (ConsistencyWatchdog, error) {
	if strings.ToLower(config.Type) != "postgres" {
		return nil, fmt.Errorf("PostgresWatchdogFactory can't instantiate watchdog of type '%s'", config.Type)
	}

	connectionString := fmt.Sprintf(connectionStringFormat, config.Props["dbname"], config.Props["user"],
															config.Props["password"], config.Props["port"],
															config.Props["conn_timeout"], config.Props["dbname"])

	connMaxLifetime, err := time.ParseDuration(config.Props["connmaxlifetime"])
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres watcher, couldn't parse 'connmaxlifetime': %s", err.Error())
	}

	maxOpenConns, err := strconv.Atoi(config.Props["maxopenconns"])
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres watcher, couldn't parse 'maxopenconns': %s", err.Error())
	}

	maxIdleConns, err := strconv.Atoi(config.Props["maxidleconns"])
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres watcher, couldn't parse 'maxidleconns': %s", err.Error())
	}

	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres watcher, couldn't connect to db: %s", err.Error())
	}

	db.SetConnMaxLifetime(connMaxLifetime)
	db.SetMaxOpenConns(maxOpenConns)
	db.SetMaxIdleConns(maxIdleConns)

	log.Printf("Postgres watcher setup successful")

	return &PostgresWatchdog{dbConn: db}, nil
}


func (watchdog *PostgresWatchdog) Insert(record *ConsistencyRecord) (*DeleteMarker, error) {
	return nil, nil
}

func (watchdog *PostgresWatchdog) Delete(marker *DeleteMarker) error {
	return nil

}
func (watchdog *PostgresWatchdog) Update(record *ConsistencyRecord) error {
	return nil
}