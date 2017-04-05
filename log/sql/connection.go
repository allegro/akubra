package sql

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"text/template"

	"github.com/Sirupsen/logrus"

	pglogrus "gopkg.in/gemnasium/logrus-postgresql-hook.v1"
)

// DBConfig holds configuration for database logging
type DBConfig struct {
	User       string `yaml:"user"`
	Password   string `yaml:"password"`
	DBName     string `yaml:"dbname"`
	Host       string `yaml:"host"`
	InsertTmpl string `yaml:"inserttmpl"`
}

// NewConnection open new db connection
func NewConnection(config DBConfig) (*sql.DB, error) {
	connStr := fmt.Sprintf("postgres://%s:%s@%s/%s",
		config.User,
		config.Password,
		config.Host,
		config.DBName)
	return sql.Open("postgres", connStr)
}

// NewSyncLogDBHook creates logrus.Hook instance with given sql.DB handler
func NewSyncLogDBHook(db *sql.DB, config DBConfig) (logrus.Hook, error) {

	tmpl, err := template.New("insert").Parse(config.InsertTmpl)
	if err != nil {
		return nil, err
	}

	hook := pglogrus.NewHook(db, make(map[string]interface{}))
	hook.InsertFunc = func(db *sql.DB, entry *logrus.Entry) error {
		query, errq := buildQuery(tmpl, entry.Message)
		if errq != nil {
			return errq
		}
		tx, errt := db.Begin()
		if errt != nil {
			return errt
		}
		_, erre := tx.Exec(query)
		if erre != nil {
			return erre
		}
		errc := tx.Commit()
		return errc
	}
	return hook, err
}

// NewSyncLogPsqlHook wraps NewSyncLogDBHook, connects to postgresql
func NewSyncLogPsqlHook(config DBConfig) (logrus.Hook, error) {
	db, err := NewConnection(config)
	if err != nil {
		return nil, err
	}
	return NewSyncLogDBHook(db, config)
}

func buildQuery(tmpl *template.Template, jsonStr string) (string, error) {
	substitutionMap := make(map[string]string)
	err := json.Unmarshal([]byte(jsonStr), &substitutionMap)
	if err != nil {
		return "", err
	}
	queryBuf := &bytes.Buffer{}
	tmpl.Execute(queryBuf, substitutionMap)
	return queryBuf.String(), nil
}
