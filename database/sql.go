package database

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/allegro/akubra/log"
	"github.com/jinzhu/gorm"
)

type DBClientFactory struct {
	dialect                   string
	connectionStringFormat    string
	connectionStringArgsNames []string
}

func NewDBClientFactory(dialect string, connectionStringFormat  string, connectionStringArgsNames []string) *DBClientFactory {
	return &DBClientFactory{
		dialect:dialect,
		connectionStringFormat: connectionStringFormat,
		connectionStringArgsNames: connectionStringArgsNames,
	}
}

func (factory *DBClientFactory) CreateConnection(dbConfig map[string]string) (*gorm.DB, error) {

	connMaxLifetime, err := time.ParseDuration(dbConfig["connmaxlifetime"])
	if err != nil {
		return nil, fmt.Errorf("failed to create DBClient, couldn't parse 'connmaxlifetime': %s", err.Error())
	}

	maxOpenConns, err := strconv.Atoi(dbConfig["maxopenconns"])
	if err != nil {
		return nil, fmt.Errorf("failed to create DBClient, couldn't parse 'maxopenconns': %s", err.Error())
	}

	maxIdleConns, err := strconv.Atoi(dbConfig["maxidleconns"])
	if err != nil {
		return nil, fmt.Errorf("failed to create DBClient, couldn't parse 'maxidleconns': %s", err.Error())
	}

	connString, err := factory.createConnString(dbConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create DBClient, couldn't prepare connection string: %s", err.Error())
	}

	db, err := gorm.Open(factory.dialect, connString)
	if err != nil {
		return nil, fmt.Errorf("failed to create DBClient, couldn't connect to db: %s", err.Error())
	}

	db.DB().SetConnMaxLifetime(connMaxLifetime)
	db.DB().SetMaxOpenConns(maxOpenConns)
	db.DB().SetMaxIdleConns(maxIdleConns)
	db.SetLogger(log.DefaultLogger)

	return db, nil
}


func (factory *DBClientFactory) createConnString(dbConfig map[string]string) (string, error) {
	connString := factory.connectionStringFormat
	for _, argName := range factory.connectionStringArgsNames {
		if argValue, isArgProvided := dbConfig[argName]; isArgProvided {
			connString = strings.Replace(connString, fmt.Sprintf(":%s:", argName), argValue, 1)
		} else {
			return "", fmt.Errorf("conn argument '%s' missing", argName)
		}
	}
	return connString, nil
}
