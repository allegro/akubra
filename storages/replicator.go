package storages

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"github.com/allegro/akubra/log"
	"github.com/allegro/akubra/storages/backend"
	"github.com/allegro/akubra/types"
)

// ErrRequestCanceled is returned if request was canceled
var ErrRequestCanceled = fmt.Errorf("Request canceled")

// ReplicationClient is multiple endpoints client
type ReplicationClient struct {
	Backends   []*backend.Backend
	cancelFunc context.CancelFunc
}

// newReplicationClient returns ReplicationClient
func newReplicationClient(backends []*backend.Backend) client {
	return &ReplicationClient{Backends: backends}
}

// Do send request to all given backends
func (rc *ReplicationClient) Do(request *http.Request) <-chan BackendResponse {
	responsesChan := make(chan BackendResponse)
	wg := sync.WaitGroup{}
	reqIDValue, ok := request.Context().Value(log.ContextreqIDKey).(string)
	if !ok {
		reqIDValue = ""
	}
	newContext := context.Background()
	newContextWithValue := context.WithValue(newContext, log.ContextreqIDKey, reqIDValue)
	ctx, cancelFunc := context.WithCancel(newContextWithValue)
	rc.cancelFunc = cancelFunc

	for _, backend := range rc.Backends {
		wg.Add(1)
		go func(backend *StorageClient) {
			requestWithContext := request.WithContext(ctx)
			if resetter, ok := request.Body.(types.Resetter); ok {
				requestWithContext.Body = resetter.Reset()
			}
			callBackend(requestWithContext, backend, responsesChan)
			wg.Done()
		}(backend)
	}

	go func() {
		wg.Wait()
		close(responsesChan)
	}()
	return responsesChan
}

// Cancel requests in progress
func (rc *ReplicationClient) Cancel() error {
	log.Debugf("ReplicationClient Cancel() called")
	if rc.cancelFunc == nil {
		return fmt.Errorf("No operation in progress cannot cancel")
	}
	rc.cancelFunc()
	return nil
}

// BackendResponse is alias of storage.types.BackendResponse
type BackendResponse = backend.Response

// StorageClient is alias of storage.types.StorageClient
type StorageClient = backend.Backend

func callBackend(request *http.Request, backend *backend.Backend, backendResponseChan chan BackendResponse) {
	resp, err := backend.RoundTrip(request)
	contextErr := request.Context().Err()
	bresp := BackendResponse{Response: resp, Error: err, Backend: backend, Request: request}

	select {
	case <-request.Context().Done():
		log.Printf("RequestContext Done %s", bresp.ReqID())
	default:
	}

	if contextErr != nil {
		bresp.Error = ErrRequestCanceled
	}

	backendResponseChan <- bresp
}
