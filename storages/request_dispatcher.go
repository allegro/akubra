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
	pickResponsePickerFactory func(*http.Request) func(<-chan BackendResponse) responsePicker
	watchdog                  watchdog.ConsistencyWatchdog
}

// NewRequestDispatcher creates RequestDispatcher instance
func NewRequestDispatcher(backends []*backend.Backend, syncLog *SyncSender, watchdog watchdog.ConsistencyWatchdog) *RequestDispatcher {
	return &RequestDispatcher{
		Backends:                  backends,
		syncLog:                   syncLog,
		pickResponsePickerFactory: defaultResponsePickerFactory,
		pickClientFactory:         defaultReplicationClientFactory,
		watchdog:                  watchdog,
	}
}

// Dispatch creates and calls replicators and response pickers
func (rd *RequestDispatcher) Dispatch(request *http.Request) (*http.Response, error) {
	storageRequest := &Request{request, nil, nil}
	if rd.watchdog != nil && !utils.IsBucketPath(request.URL.Path) && len(rd.Backends) > 1 {
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
	pickerFactory := rd.pickResponsePickerFactory(request)
	pickr := pickerFactory(respChan)
	go pickr.SendSyncLog(rd.syncLog)
	return pickr.Pick()
}
func (rd *RequestDispatcher) createAndInsertRecordFor(request *http.Request) (*Request, error) {
	record, err := watchdog.CreateRecordFor(request)
	if err != nil {
		return nil, err
	}
	if isInitiateMultipartUploadRequest(request) {
		record.ExecutionDate = record.ExecutionDate.Add(oneWeek)
	}
	deleteMarker, err := rd.watchdog.Insert(record)
	if err != nil {
		return nil, err
	}
	return &Request{
		request,
		record,
		deleteMarker,
	}, nil
}

type responsePicker interface {
	Pick() (*http.Response, error)
	SendSyncLog(*SyncSender)
}

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

var defaultResponsePickerFactory = func(request *http.Request) func(<-chan BackendResponse) responsePicker {
	if utils.IsBucketPath(request.URL.Path) && (request.Method == http.MethodGet) {
		return newResponseHandler
	}

	if utils.IsBucketPath(request.URL.Path) && ((request.Method == http.MethodPut) || (request.Method == http.MethodDelete)) {
		return newDeleteResponsePicker
	}

	if request.Method == http.MethodDelete {
		return newDeleteResponsePicker
	}
	return newObjectResponsePicker
}
