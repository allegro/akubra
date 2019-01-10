package watchdog

import (
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/allegro/akubra/database"
	"github.com/allegro/akubra/log"
	"github.com/allegro/akubra/utils"
)

const (
	fiveMinutes = time.Minute * 5
	oneWeek = time.Hour * 24 * 7
	// Domain is a constant used to put/get domain's name from request's context
	Domain = log.ContextKey("Domain")
)

const (
	// PUT consistency method states that an object should be present
	PUT Method = "PUT"
	// DELETE consistency method states that an object should be deleted
	DELETE Method = "DELETE"
)

// ConsistencyRecord describes the state of an object in domain
type ConsistencyRecord struct {
	sync.Mutex

	RequestID      string
	ExecutionDelay time.Duration
	ObjectID       string
	Method         Method
	Domain         string
	AccessKey      string
	ObjectVersion  string

	isReflectedOnBackends bool
}

func (record *ConsistencyRecord) GetObjectVersion() string {
	return record.ObjectVersion
}

// DeleteMarker indicates which ConsistencyRecords for a given object can be deleted
type DeleteMarker struct {
	objectID      string
	domain        string
	insertionDate time.Time
}

//ExecutionDelay tells how to change the execution time of a record
type ExecutionDelay struct {
	RequestID string
	Delay     time.Duration
}

// ConsistencyWatchdog manages the ConsistencyRecords and DeleteMarkers
type ConsistencyWatchdog interface {
	Insert(record *ConsistencyRecord) (*DeleteMarker, error)
	InsertWithRequestID(requestID string, record *ConsistencyRecord) (*DeleteMarker, error)
	Delete(marker *DeleteMarker) error
	UpdateExecutionDelay(delta *ExecutionDelay) error
	SupplyRecordWithVersion(record *ConsistencyRecord) error
}

// ConsistencyRecordFactory creates records from http requests
type ConsistencyRecordFactory interface {
	CreateRecordFor(request *http.Request) (*ConsistencyRecord, error)
}

// DefaultConsistencyRecordFactory is a default implementation of ConsistencyRecordFactory
type DefaultConsistencyRecordFactory struct {
}

// CreateSQL creates ConsistencyWatchdog and ConsistencyRecordFactory that make use of a SQL database
func CreateSQL(dialect, connStringFormat string, params []string, watchdogConfig *Config) (ConsistencyWatchdog, error) {
	sqlWatchdogFactory := CreateSQLWatchdogFactory(database.NewDBClientFactory(dialect, connStringFormat, params))
	watchdog, err := sqlWatchdogFactory.CreateWatchdogInstance(watchdogConfig)
	if err != nil {
		return nil, err
	}
	return watchdog, nil
}

// CreateRecordFor creates a ConsistencyRecord from a http request
func (factory *DefaultConsistencyRecordFactory) CreateRecordFor(request *http.Request) (*ConsistencyRecord, error) {
	var method Method
	switch request.Method {
	case "PUT", "POST":
		method = PUT
		break
	case "DELETE":
		method = DELETE
		break
	default:
		return nil, fmt.Errorf("unsupported method - %s", request.Method)
	}

	bucket, key := utils.ExtractBucketAndKey(request.URL.Path)
	if bucket == "" || key == "" {
		return nil, errors.New("failed to extract bucket/key from path")
	}

	accessKey := utils.ExtractAccessKey(request)
	if accessKey == "" {
		return nil, errors.New("failed to extract access key")
	}

	domain, domainPresent := request.Context().Value(Domain).(string)
	if !domainPresent {
		return nil, errors.New("domain name is not present in context")
	}

	requestID, reqIDPresent := request.Context().Value(log.ContextreqIDKey).(string)
	if !reqIDPresent {
		return nil, errors.New("reqID name is not present in context")
	}

	executionDelay := fiveMinutes
	if utils.IsMultiPartUploadRequest(request) {
		executionDelay = oneWeek
	}

	return &ConsistencyRecord{
		RequestID:             requestID,
		ExecutionDelay:        executionDelay,
		ObjectID:              fmt.Sprintf("%s/%s", bucket, key),
		AccessKey:             accessKey,
		Domain:                domain,
		isReflectedOnBackends: true,
		Method:                method,
	}, nil
}

// AddBackendResult combines backend's response with the previous responses
func (record *ConsistencyRecord) AddBackendResult(wasSuccessfullOnBackend bool) {
	record.Lock()
	defer record.Unlock()
	record.isReflectedOnBackends = record.isReflectedOnBackends && wasSuccessfullOnBackend
}

// IsReflectedOnAllStorages tell wheter the request was successfull on all backends
func (record *ConsistencyRecord) IsReflectedOnAllStorages() bool {
	record.Lock()
	defer record.Unlock()
	return record.isReflectedOnBackends
}