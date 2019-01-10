package storages

import (
	"fmt"
	"net/http"

	"github.com/allegro/akubra/storages/backend"
	"github.com/allegro/akubra/utils"
	"github.com/allegro/akubra/watchdog"
)

type dispatcher interface {
	Dispatch(request *http.Request) (*http.Response, error)
}

// RequestDispatcher passes requests and responses to matching replicators and response pickers
type RequestDispatcher struct {
	Backends                        []*backend.Backend
	syncLog                         *SyncSender
	pickClientFactory               func(*http.Request) func([]*backend.Backend, watchdog.ConsistencyWatchdog) client
	pickResponsePickerFactory       func(*Request) func(<-chan BackendResponse) responsePicker
	watchdog                        watchdog.ConsistencyWatchdog
	objectWatchdogVersionHeaderName string
	watchdogRecordFactory           watchdog.ConsistencyRecordFactory
}

// NewRequestDispatcher creates RequestDispatcher instance
func NewRequestDispatcher(
	backends []*backend.Backend,
	syncLog *SyncSender,
	watchdog watchdog.ConsistencyWatchdog,
	objectWatchdogVersionHeaderName string,
	watchdogRecordFactory watchdog.ConsistencyRecordFactory) *RequestDispatcher {

	return &RequestDispatcher{
		Backends:                  backends,
		syncLog:                   syncLog,
		pickResponsePickerFactory: defaultResponsePickerFactory,
		pickClientFactory:         defaultReplicationClientFactory,
		watchdog:                  watchdog,
		watchdogRecordFactory:     watchdogRecordFactory,
		objectWatchdogVersionHeaderName: objectWatchdogVersionHeaderName,
	}
}

// Dispatch creates and calls replicators and response pickers
func (rd *RequestDispatcher) Dispatch(request *http.Request) (*http.Response, error) {
	storageRequest := &Request{
		Request:                          request,
		isMultiPartUploadRequest:         utils.IsMultiPartUploadRequest(request),
		isInitiateMultipartUploadRequest: utils.IsInitiateMultiPartUploadRequest(request),
	}
	var err error
	if rd.shouldUseConsistencyWatchdogFor(storageRequest) {
		storageRequest, err = rd.recordRequest(storageRequest)
		if err != nil {
			return nil, err
		}
		storageRequest.Header.Add(rd.objectWatchdogVersionHeaderName, storageRequest.record.GetObjectVersion())
	}

	clientFactory := rd.pickClientFactory(request)
	cli := clientFactory(rd.Backends, rd.watchdog)
	respChan := cli.Do(storageRequest)
	pickerFactory := rd.pickResponsePickerFactory(storageRequest)
	pickr := pickerFactory(respChan)
	go pickr.SendSyncLog(rd.syncLog)
	resp, err := pickr.Pick()
	if err != nil {
		return nil, err
	}
	if storageRequest.record != nil && storageRequest.isInitiateMultipartUploadRequest {
		if rd.recordMultipart(storageRequest, resp) != nil {
			return nil, err
		}
	}
	return resp, nil
}
func (rd *RequestDispatcher) shouldUseConsistencyWatchdogFor(request *Request) bool {
	consistencyCondition := rd.watchdog != nil && len(rd.Backends) > 1

	methodCondition := (http.MethodPut == request.Method && !request.isMultiPartUploadRequest) ||
		http.MethodDelete == request.Method ||
		(http.MethodPost == request.Method && request.isInitiateMultipartUploadRequest)

	pathCondition := !utils.IsBucketPath(request.URL.Path)

	return consistencyCondition && methodCondition && pathCondition
}
func (rd *RequestDispatcher) recordRequest(storageRequest *Request) (*Request, error) {
	consistencyRecord, err := rd.watchdogRecordFactory.CreateRecordFor(storageRequest.Request)
	if err != nil {
		return nil, err
	}
	storageRequest.record = consistencyRecord

	if storageRequest.isInitiateMultipartUploadRequest {
		err = rd.watchdog.SupplyRecordWithVersion(consistencyRecord)
		if err != nil {
			return nil, err
		}
	} else {
		deleteMarker, err := rd.watchdog.Insert(consistencyRecord)
		if err != nil {
			return nil, err
		}
		storageRequest.marker = deleteMarker
	}

	return storageRequest, nil
}
func (rd *RequestDispatcher) recordMultipart(storageRequest *Request, resp *http.Response) error {
	multiPartUploadID, err := utils.ExtractMultiPartUploadIDFrom(resp)
	if err != nil {
		return fmt.Errorf("failed on extracting multipart upload ID from response: %s", err)
	}
	_, err = rd.watchdog.InsertWithRequestID(multiPartUploadID, storageRequest.record)
	if err != nil {
		return err
	}
	return nil
}

type responsePicker interface {
	Pick() (*http.Response, error)
	SendSyncLog(*SyncSender)
}

// Request encapsulates the http requests along with the watchdog-data
type Request struct {
	*http.Request
	record                           *watchdog.ConsistencyRecord
	marker                           *watchdog.DeleteMarker
	isMultiPartUploadRequest         bool
	isInitiateMultipartUploadRequest bool
}

type client interface {
	Do(request *Request) <-chan BackendResponse
	Cancel() error
}

var defaultReplicationClientFactory = func(request *http.Request) func([]*backend.Backend, watchdog.ConsistencyWatchdog) client {
	if utils.IsMultiPartUploadRequest(request) {
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
