package watchdog

import (
	"errors"
	"fmt"
	"github.com/allegro/akubra/internal/akubra/httphandler"
	"github.com/allegro/akubra/internal/akubra/watchdog/config"
	"net/http"
	"time"

	"github.com/allegro/akubra/internal/akubra/log"
	"github.com/allegro/akubra/internal/akubra/utils"
)

const (
	fiveMinutes = time.Minute * 5
	oneWeek     = time.Hour * 24 * 7
	// ConsistencyLevel is a constant used to put/get policy consistency level to/from request's context
	ConsistencyLevel = log.ContextKey("ConsistencyLevel")
	// ReadRepair is a constant used to put/get policy read repair to/from request's context
	ReadRepair = log.ContextKey("ReadRepair")
	//ReadRepairObjectVersion tells that watchdog should insert a read-repair record
	ReadRepairObjectVersion = log.ContextKey("ReadRepairObjectVersion")
	//NoErrorsDuringRequest indicates that all of the storages requests were successful
	NoErrorsDuringRequest = log.ContextKey("NoErrorsDuringProcessing")
	//MultiPartUpload indicates that the request was a finish multipart upload request and the whole multipart was ok
	MultiPartUpload = log.ContextKey("MultiPartUpload")
)

const (
	// PUT consistency method states that an object should be present
	PUT Method = "PUT"
	// DELETE consistency method states that an object should be deleted
	DELETE Method = "DELETE"
)

// Method is the ConsistencyRecord type
type Method string

// ConsistencyWatchdogFactory creates ConsistencyWatchdogs
type ConsistencyWatchdogFactory interface {
	CreateWatchdogInstance(config *config.WatchdogConfig) (ConsistencyWatchdog, error)
}

// ConsistencyRecord describes the state of an object in domain
type ConsistencyRecord struct {
	RequestID      string
	ExecutionDelay time.Duration
	ObjectID       string
	Method         Method
	Domain         string
	AccessKey      string
	ObjectVersion  int
}

// DeleteMarker indicates which ConsistencyRecords for a given object can be deleted
type DeleteMarker struct {
	objectID      string
	domain        string
	objectVersion int
}

//ExecutionDelay tells how to change the execution time of a record
type ExecutionDelay struct {
	RequestID string
	Delay     time.Duration
}

// ConsistencyWatchdog manages the ConsistencyRecords and DeleteMarkers
type ConsistencyWatchdog interface {
	Insert(record *ConsistencyRecord) (*DeleteMarker, error)
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

// CreateRecordFor creates a ConsistencyRecord from a http request
func (factory *DefaultConsistencyRecordFactory) CreateRecordFor(request *http.Request) (*ConsistencyRecord, error) {
	var method Method
	switch request.Method {
	case "PUT", "POST", "GET", "HEAD":
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

	domain, domainPresent := request.Context().Value(httphandler.Domain).(string)
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
		RequestID:      requestID,
		ExecutionDelay: executionDelay,
		ObjectID:       fmt.Sprintf("%s/%s", bucket, key),
		AccessKey:      accessKey,
		Domain:         domain,
		Method:         method,
	}, nil
}
