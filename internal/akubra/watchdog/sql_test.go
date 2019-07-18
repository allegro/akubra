package watchdog

import (
	"database/sql"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jinzhu/gorm"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestShouldAddConsistencyRecordForRequestAndReturnTheObjectVersionGeneratedByDatabase(t *testing.T) {
	_, dbMock, gormDbMock := createDBMock(t)
	watchdog := SQLWatchdog{dbConn: gormDbMock, versionHeaderName: "x-version-header"}

	expectedObjectVersion := 123
	record := ConsistencyRecord{
		RequestID: "1",
		ObjectID: "bucket/key",
		AccessKey: "access",
		ExecutionDelay: fiveMinutes,
		Domain: "local.qxlint",
		Method: PUT,
	}

	dbMock.
		ExpectQuery(`INSERT\ INTO\ consistency_record\ \(request_id\,\ object_id\,\ domain\,\ access_key\,\ execution_delay\,\ method\)\ VALUES\ .+\ RETURNING\ object_version`).
		WithArgs(record.RequestID, record.ObjectID, record.Domain, record.AccessKey, record.ExecutionDelay.String(), record.Method).
		WillReturnRows(sqlmock.NewRows([]string{"object_version"}).AddRow(expectedObjectVersion)).
		WillReturnError(nil).
		RowsWillBeClosed()
	
	deleteMarker, err := watchdog.Insert(&record)

	assert.Equal(t, deleteMarker.objectVersion, expectedObjectVersion)
	assert.Equal(t, deleteMarker.objectID, record.ObjectID)
	assert.Equal(t, deleteMarker.domain, record.Domain)
	assert.Nil(t, err)
}

func TestShouldAddConsistencyRecordForRequestWithTheVersionSuppliedInTheRecord(t *testing.T) {
	_, dbMock, gormDbMock := createDBMock(t)
	watchdog := SQLWatchdog{dbConn: gormDbMock, versionHeaderName: "x-version-header"}

	expectedObjectVersion := 123
	record := ConsistencyRecord{
		RequestID: "1",
		ObjectID: "bucket/key",
		AccessKey: "access",
		ExecutionDelay: fiveMinutes,
		Domain: "local.local",
		Method: PUT,
		ObjectVersion: expectedObjectVersion,
	}

	dbMock.
		ExpectQuery(`INSERT\ INTO\ consistency_record\ \(object_version\,\ request_id\,\ object_id\,\ domain\,\ access_key\,\ execution_delay\,\ method\)\ VALUES\ .+\ RETURNING\ object_version`).
		WithArgs(expectedObjectVersion, record.RequestID, record.ObjectID, record.Domain, record.AccessKey, record.ExecutionDelay.String(), record.Method).
		WillReturnRows(sqlmock.NewRows([]string{"object_version"}).AddRow(expectedObjectVersion)).
		WillReturnError(nil).
		RowsWillBeClosed()

	deleteMarker, err := watchdog.Insert(&record)

	assert.Equal(t, deleteMarker.objectVersion, expectedObjectVersion)
	assert.Equal(t, deleteMarker.objectID, record.ObjectID)
	assert.Equal(t, deleteMarker.domain, record.Domain)
	assert.Nil(t, err)
	assert.Nil(t, dbMock.ExpectationsWereMet())
}

func TestShouldSupplyRecordWithObjectVersion(t *testing.T) {
	_, dbMock, gormDbMock := createDBMock(t)
	watchdog := SQLWatchdog{dbConn: gormDbMock, versionHeaderName: "x-version-header"}

	expectedObjectVersion := 123
	record := ConsistencyRecord{
		RequestID: "1",
		ObjectID: "bucket/key",
		AccessKey: "access",
		ExecutionDelay: fiveMinutes,
		Domain: "local.com",
		Method: PUT,
		ObjectVersion: expectedObjectVersion,
	}

	dbMock.
		ExpectQuery(`SELECT\ CAST\(EXTRACT\(EPOCH\ FROM\ CURRENT_TIMESTAMP\ at\ time\ zone\ 'utc'\)\ \*\ 10\^6\ AS\ BIGINT\)`).
		WillReturnRows(sqlmock.NewRows([]string{"now"}).AddRow(expectedObjectVersion)).
		WillReturnError(nil).
		RowsWillBeClosed()

	err := watchdog.SupplyRecordWithVersion(&record)

	assert.Nil(t, err)
	assert.Equal(t, expectedObjectVersion, record.ObjectVersion)
	assert.Nil(t, dbMock.ExpectationsWereMet())
}

func TestShouldDeleteRecordsByMarker(t *testing.T) {
	_, dbMock, gormDbMock := createDBMock(t)
	watchdog := SQLWatchdog{dbConn: gormDbMock, versionHeaderName: "x-version-header"}

	marker := DeleteMarker{
		domain:"domain.local",
		objectID: "key/bucket",
		objectVersion: 123,
	}

	dbMock.
		ExpectExec(`DELETE\ FROM\ "consistency_record"\ WHERE\ \(domain\ \=\ .+\ AND\ object_id\ \=\ .+\ AND\ object_version\ \<\=\ .+\)`).
		WithArgs(marker.domain, marker.objectID, marker.objectVersion).
		WillReturnResult(sqlmock.NewResult(1, 1)).
		WillReturnError(nil)

	err := watchdog.Delete(&marker)
	assert.Nil(t, err)
}


func createDBMock(t *testing.T) (*sql.DB, sqlmock.Sqlmock, *gorm.DB) {
	db, dbMock, err := sqlmock.New()
	assert.NoError(t, err)
	gormDB, err := gorm.Open("postgres", db)
	assert.NoError(t, err)

	return db, dbMock, gormDB
}