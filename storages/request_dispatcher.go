package storages

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/allegro/akubra/regions/config"
	"github.com/allegro/akubra/storages/backend"
	"github.com/allegro/akubra/utils"
	"github.com/allegro/akubra/watchdog"
)

type dispatcher interface {
	Dispatch(request *Request) (*http.Response, error)
}

// RequestDispatcher passes requests and responses to matching replicators and response pickers
type RequestDispatcher struct {
	Backends                  []*backend.Backend
	pickClientFactory         func(*http.Request) func([]*backend.Backend, watchdog.ConsistencyWatchdog) client
	pickResponsePickerFactory func(*Request) func(<-chan BackendResponse) responsePicker
	watchdog                  watchdog.ConsistencyWatchdog
	watchdogRecordFactory     watchdog.ConsistencyRecordFactory
}

// NewRequestDispatcher creates RequestDispatcher instance
func NewRequestDispatcher(
	backends []*backend.Backend,
	watchdog watchdog.ConsistencyWatchdog,
	watchdogRecordFactory watchdog.ConsistencyRecordFactory) *RequestDispatcher {

	return &RequestDispatcher{
		Backends:                  backends,
		pickResponsePickerFactory: defaultResponsePickerFactory,
		pickClientFactory:         defaultReplicationClientFactory,
		watchdog:                  watchdog,
		watchdogRecordFactory:     watchdogRecordFactory,
	}
}

// Dispatch creates and calls replicators and response pickers
func (rd *RequestDispatcher) Dispatch(request *Request) (*http.Response, error) {
	consistencyLevel, _, err := extractRegionPropsFrom(request)
	if err != nil {
		return nil, err
	}
	request, err = rd.ensureConsistency(request, consistencyLevel)
	if err != nil {
		return nil, err
	}

	clientFactory := rd.pickClientFactory(request.Request)
	cli := clientFactory(rd.Backends, rd.watchdog)

	respChan := cli.Do(request)
	pickerFactory := rd.pickResponsePickerFactory(request)
	pickr := pickerFactory(respChan)

	resp, err := pickr.Pick()
	if err != nil {
		return nil, err
	}

	return rd.logIfInitMultiPart(request, resp, consistencyLevel)
}
func extractRegionPropsFrom(request *Request) (config.ConsistencyLevel, bool, error) {
	consistencyLevelProp := request.Context().Value(watchdog.ConsistencyLevel)
	if consistencyLevelProp == nil {
		return "", false, errors.New("'ConsistencyLevel' not present in request's context")
	}
	consistencyLevel, castOk := consistencyLevelProp.(config.ConsistencyLevel)
	if !castOk {
		return "", false, errors.New("couldn't determine consistency level of region")

	}
	readRepairProp := request.Context().Value(watchdog.ReadRepair)
	if readRepairProp == nil {
		return "", false, errors.New("'ReadRepair' not present in request's context")
	}
	readRepair, castOk := readRepairProp.(bool)
	if !castOk {
		return "", false, errors.New("couldn't if read reapair should be used")

	}
	return consistencyLevel, readRepair, nil
}
func (rd *RequestDispatcher) shouldLogRequest(request *Request, level config.ConsistencyLevel) bool {
	isObjectPath := !utils.IsBucketPath(request.URL.Path)

	if rd.watchdog == nil {
		return false
	}
	if http.MethodDelete == request.Method && isObjectPath{
		return true
	}
	if level == config.None {
		return false
	}
	isPutOrInitMultiPart := (http.MethodPut == request.Method && !request.isMultiPartUploadRequest) ||
		(http.MethodPost == request.Method && request.isInitiateMultipartUploadRequest)

	return isPutOrInitMultiPart && isObjectPath
}

func (rd *RequestDispatcher) logRequest(storageRequest *Request) (*Request, error) {
	if storageRequest.isInitiateMultipartUploadRequest {
		err := rd.watchdog.SupplyRecordWithVersion(storageRequest.logRecord)
		if err != nil {
			return nil, err
		}
	} else {
		deleteMarker, err := rd.watchdog.Insert(storageRequest.logRecord)
		if err != nil {
			return storageRequest, err
		}
		storageRequest.marker = deleteMarker
	}

	if storageRequest.Method != http.MethodDelete {
		storageRequest.
			Header.
			Add(rd.watchdog.GetVersionHeaderName(), storageRequest.logRecord.ObjectVersion)
	}

	return storageRequest, nil
}

func (rd *RequestDispatcher) logMultipart(storageRequest *Request, resp *http.Response) error {
	multiPartUploadID, err := utils.ExtractMultiPartUploadIDFrom(resp)
	if err != nil {
		return fmt.Errorf("failed on extracting multipart upload ID from response: %s", err)
	}
	_, err = rd.watchdog.InsertWithRequestID(multiPartUploadID, storageRequest.logRecord)
	if err != nil {
		return err
	}
	return nil
}

func (rd *RequestDispatcher) ensureConsistency(storageRequest *Request, consistencyLevel config.ConsistencyLevel) (*Request, error) {
	if !rd.shouldLogRequest(storageRequest, consistencyLevel) {
		return storageRequest, nil
	}

	consistencyRecord, err := rd.
		watchdogRecordFactory.
		CreateRecordFor(storageRequest.Request)
	storageRequest.logRecord = consistencyRecord

	if err != nil {
		if config.Strong == consistencyLevel {
			return nil, err
		}
		return storageRequest, nil
	}

	loggedRequest, err := rd.logRequest(storageRequest)
	if err != nil {
		if config.Strong == consistencyLevel {
			return nil, err
		}
		return storageRequest, nil
	}
	return loggedRequest, nil
}

func (rd *RequestDispatcher) logIfInitMultiPart(request *Request, response *http.Response, consistencyLevel config.ConsistencyLevel) (*http.Response, error) {
	if consistencyLevel != config.None && request.isInitiateMultipartUploadRequest {
		err := rd.logMultipart(request, response)
		if err != nil && consistencyLevel == config.Strong {
			return nil, err
		}
	}
	return response, nil
}

type responsePicker interface {
	Pick() (*http.Response, error)
}

// Request encapsulates the http requests along with the watchdog-data
type Request struct {
	*http.Request
	logRecord                        *watchdog.ConsistencyRecord
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
		return newAllResponsesSuccessfulPicker
	}
	if request.Method == http.MethodDelete {
		return newFirstSuccessfulResponsePicker
	}
	return newFirstSuccessfulResponsePicker
}
