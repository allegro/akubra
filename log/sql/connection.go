package sql

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"text/template"

	"github.com/Sirupsen/logrus"
	_ "github.com/lib/pq"
	pglogrus "gopkg.in/gemnasium/logrus-postgresql-hook.v1"
)

type DBConfig struct {
	User       string `yaml:"user"`
	Password   string `yaml:"password"`
	DBName     string `yaml:"dbname"`
	Host       string `yaml:"host"`
	InsertTmpl string `yaml:"inserttmpl"`
}

func NewConnection(config DBConfig) (*sql.DB, error) {
	connStr := fmt.Sprintf("postgres://%s:%s@%s/%s",
		config.User,
		config.Password,
		config.Host,
		config.DBName)
	return sql.Open("postgres", connStr)
}

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

func NewSyncLogDBHook(db *sql.DB, config DBConfig) (logrus.Hook, error) {

	tmpl, err := template.New("insert").Parse(config.InsertTmpl)
	if err != nil {
		return nil, err
	}

	hook := pglogrus.NewHook(db, make(map[string]interface{}))
	hook.InsertFunc = func(db *sql.DB, entry *logrus.Entry) error {
		query, err := buildQuery(tmpl, entry.Message)
		if err != nil {
			return err
		}
		tx, err := db.Begin()
		if err != nil {
			return err
		}
		_, err = tx.Exec(query)
		if err != nil {
			return err
		}
		err = tx.Commit()
		return err
	}
	return hook, err
}
