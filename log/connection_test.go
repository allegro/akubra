package log

import (
	"fmt"
	"testing"

	"github.com/Sirupsen/logrus"
	"github.com/allegro/akubra/log/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

var loggerConf = LoggerConfig{
	PlainText: true,
}

var dbConf = sql.DBConfig{
	User:       "test",
	Password:   "test",
	DBName:     "test",
	Host:       "localhost",
	InsertTmpl: "insert into entry(source, dest) values ('{{.source}}', '{{.dest}}')",
}

func TestFakeDBHook(t *testing.T) {
	logger, err := NewLogger(loggerConf)
	require.NoError(t, err)
	lslogger, ok := logger.(*logrus.Logger)
	require.True(t, ok)

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	hook, err := sql.NewSyncLogDBHook(db, dbConf)
	require.NoError(t, err)

	lslogger.Hooks[logrus.InfoLevel] = []logrus.Hook{hook}
	result := sqlmock.NewResult(1, 1)

	mock.ExpectBegin()
	mock.ExpectExec("insert into entry\\(source, dest\\) values.*").WillReturnResult(result)
	mock.ExpectCommit()

	logger.Println(fmt.Sprintf("{%q:%q, %q:%q}", "source", "ss", "dest", "dd"))

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)

}
