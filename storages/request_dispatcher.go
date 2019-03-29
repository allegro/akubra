package storages

import (
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
	Backends                  []*backend.Backend
	pickClientFactory         func(*http.Request) func([]*backend.Backend) client
	pickResponsePickerFactory func(*http.Request) func(<-chan BackendResponse) responsePicker
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
func (rd *RequestDispatcher) Dispatch(request *http.Request) (*http.Response, error) {
	clientFactory := rd.pickClientFactory(request)
	cli := clientFactory(rd.Backends)

	respChan := cli.Do(request)
	pickerFactory := rd.pickResponsePickerFactory(request)
	pickr := pickerFactory(respChan)

	resp, err := pickr.Pick()
	if err != nil {
		return nil, err
	}
	return resp, err
}

type responsePicker interface {
	Pick() (*http.Response, error)
}

type client interface {
	Do(request *http.Request) <-chan BackendResponse
	Cancel() error
}

var defaultReplicationClientFactory = func(request *http.Request) func([]*backend.Backend) client {
	if utils.IsMultiPartUploadRequest(request) {
		return newMultiPartRoundTripper
	}
	return newReplicationClient
}

var defaultResponsePickerFactory = func(request *http.Request) func(<-chan BackendResponse) responsePicker {
	if utils.IsBucketPath(request.URL.Path) && (request.Method == http.MethodGet) {
		return newResponseHandler
	}
	if utils.IsBucketPath(request.URL.Path) && ((request.Method == http.MethodPut) || (request.Method == http.MethodDelete)) {
		return newAllResponsesSuccessfulPicker
	}
	return newFirstSuccessfulResponsePicker
}
