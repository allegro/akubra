package storages

import (
	"net/http"
	"time"

	"github.com/allegro/akubra/storages/backend"
	"github.com/allegro/akubra/utils"
	"github.com/allegro/akubra/watchdog"
)

const (
	oneWeek = time.Hour * 24 * 7
)

type dispatcher interface {
	Dispatch(request *http.Request) (*http.Response, error)
}

// RequestDispatcher passes requests and responses to matching replicators and response pickers
type RequestDispatcher struct {
	Backends                  []*backend.Backend
	syncLog                   *SyncSender
	pickClientFactory         func(*http.Request) func([]*backend.Backend, watchdog.ConsistencyWatchdog) client
	pickResponsePickerFactory func(*Request) func(<-chan BackendResponse) responsePicker
	watchdog                  watchdog.ConsistencyWatchdog
	watchdogRecordFactory     watchdog.ConsistencyRecordFactory
}

// NewRequestDispatcher creates RequestDispatcher instance
func NewRequestDispatcher(backends []*backend.Backend, syncLog *SyncSender,
	watchdog watchdog.ConsistencyWatchdog, watchdogRecordFactory watchdog.ConsistencyRecordFactory) *RequestDispatcher {
	return &RequestDispatcher{
		Backends:                  backends,
		syncLog:                   syncLog,
		pickResponsePickerFactory: defaultResponsePickerFactory,
		pickClientFactory:         defaultReplicationClientFactory,
		watchdog:                  watchdog,
		watchdogRecordFactory:     watchdogRecordFactory,
	}
}

// Dispatch creates and calls replicators and response pickers
func (rd *RequestDispatcher) Dispatch(request *http.Request) (*http.Response, error) {
	storageRequest := &Request{Request: request}
	if rd.shouldUseConsistencyWatchdog(request) {
		recordedRequest, err := rd.createAndInsertRecordFor(request)
		if err != nil {
			return nil, err
		}
		storageRequest.record = recordedRequest.record
		storageRequest.marker = recordedRequest.marker
	}
	clientFactory := rd.pickClientFactory(request)
	cli := clientFactory(rd.Backends, rd.watchdog)
	respChan := cli.Do(storageRequest)
	pickerFactory := rd.pickResponsePickerFactory(storageRequest)
	pickr := pickerFactory(respChan)
	go pickr.SendSyncLog(rd.syncLog)
	return pickr.Pick()
}
func (rd *RequestDispatcher) createAndInsertRecordFor(request *http.Request) (*Request, error) {
	record, err := rd.watchdogRecordFactory.CreateRecordFor(request)
	if err != nil {
		return nil, err
	}
	if isInitiateMultiPartUploadRequest(request) {
		record.ExecutionDate = record.ExecutionDate.Add(oneWeek).Add(time.Minute * 10)
	}
	deleteMarker, err := rd.watchdog.Insert(record)
	if err != nil {
		return nil, err
	}
	return &Request{
		Request: request,
		record: record,
		marker: deleteMarker,
	}, nil
}
func (rd *RequestDispatcher) shouldUseConsistencyWatchdog(request *http.Request) bool {
	isMultiPartRequest := isMultiPartUploadRequest(request)
	isInitiateMultiPart := isInitiateMultiPartUploadRequest(request)

	consistencyCondition := rd.watchdog != nil && len(rd.Backends) > 1

	methodCondition := (http.MethodPut == request.Method && !isMultiPartRequest) ||
						http.MethodDelete == request.Method ||
						(http.MethodPost == request.Method && isInitiateMultiPart)

	pathCondition := !utils.IsBucketPath(request.URL.Path)

	return consistencyCondition && methodCondition && pathCondition
}

type responsePicker interface {
	Pick() (*http.Response, error)
	SendSyncLog(*SyncSender)
}

// Request encapsulates the http requests along with the watchdog-data
type Request struct {
	*http.Request
	record *watchdog.ConsistencyRecord
	marker *watchdog.DeleteMarker
}

type client interface {
	Do(request *Request) <-chan BackendResponse
	Cancel() error
}

var defaultReplicationClientFactory = func(request *http.Request) func([]*backend.Backend, watchdog.ConsistencyWatchdog) client {
	if isMultiPartUploadRequest(request) {
		return newMultiPartRoundTripper
	}
	return newReplicationClient
}

var defaultResponsePickerFactory = func(request *Request) func(<-chan BackendResponse) responsePicker {
	if utils.IsBucketPath(request.URL.Path) && (request.Method == http.MethodGet) {
		return newResponseHandler
	}

	if utils.IsBucketPath(request.URL.Path) && ((request.Method == http.MethodPut) || (request.Method == http.MethodDelete)) {
		return newDeleteResponsePicker
	}

	if request.Method == http.MethodDelete {
		if request.record != nil {
			return newDeleteResponsePickerWatchdog
		}
		return newDeleteResponsePicker
	}

	return newObjectResponsePicker
}
