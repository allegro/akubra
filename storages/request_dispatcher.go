package storages

import (
	"net/http"

	"github.com/allegro/akubra/storages/backend"
)

type dispatcher interface {
	Dispatch(request *http.Request) (*http.Response, error)
}

// RequestDispatcher passes requests and responses to matching replicators and response pickers
type RequestDispatcher struct {
	Backends                  []*backend.Backend
	syncLog                   *SyncSender
	pickClientFactory         func(*http.Request) func([]*backend.Backend) client
	pickResponsePickerFactory func(*http.Request) func(<-chan BackendResponse) responsePicker
}

// NewRequestDispatcher creates RequestDispatcher instance
func NewRequestDispatcher(backends []*backend.Backend, syncLog *SyncSender) *RequestDispatcher {

	return &RequestDispatcher{
		Backends:                  backends,
		syncLog:                   syncLog,
		pickResponsePickerFactory: defaultResponsePickerFactory,
		pickClientFactory:         defaultReplicationClientFactory,
	}
}

// Dispatch creates and calls replicators and response pickers
func (rd *RequestDispatcher) Dispatch(request *http.Request) (*http.Response, error) {
	clientFactory := rd.pickClientFactory(request)
	cli := clientFactory(rd.Backends)
	respChan := cli.Do(request)
	pickerFactory := rd.pickResponsePickerFactory(request)
	pickr := pickerFactory(respChan)
	go pickr.SendSyncLog(rd.syncLog)
	return pickr.Pick()
}

type responsePicker interface {
	Pick() (*http.Response, error)
	SendSyncLog(*SyncSender)
}

type client interface {
	Do(*http.Request) <-chan BackendResponse
	Cancel() error
}

var defaultReplicationClientFactory = func(request *http.Request) func([]*backend.Backend) client {
	if isMultiPartUploadRequest(request) {
		return newMultiPartRoundTripper
	}
	return newReplicationClient
}

var defaultResponsePickerFactory = func(request *http.Request) func(<-chan BackendResponse) responsePicker {
	if isBucketPath(request.URL.Path) && (request.Method == http.MethodGet) {
		return newResponseHandler
	}

	if isBucketPath(request.URL.Path) && ((request.Method == http.MethodPut) || (request.Method == http.MethodDelete)) {
		return newDeleteResponsePicker
	}

	if request.Method == http.MethodDelete {
		return newDeleteResponsePicker
	}
	return newObjectResponsePicker
}
