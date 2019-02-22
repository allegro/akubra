package storages

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"github.com/allegro/akubra/log"
	"github.com/allegro/akubra/storages/backend"
	"github.com/allegro/akubra/types"
	"github.com/allegro/akubra/watchdog"
)

// ErrRequestCanceled is returned if request was canceled
var ErrRequestCanceled = fmt.Errorf("Request canceled")

// ReplicationClient is multiple endpoints client
type ReplicationClient struct {
	Backends   []*backend.Backend
	cancelFunc context.CancelFunc
	watchdog   watchdog.ConsistencyWatchdog
}

// newReplicationClient returns ReplicationClient
func newReplicationClient(backends []*backend.Backend, watchdog watchdog.ConsistencyWatchdog) client {
	return &ReplicationClient{Backends: backends, watchdog: watchdog}
}

// Do send request to all given backends
func (rc *ReplicationClient) Do(request *Request) <-chan BackendResponse {
	reqIDValue, ok := request.Context().Value(log.ContextreqIDKey).(string)
	if !ok {
		reqIDValue = ""
	}
	responsesChan := make(chan BackendResponse)
	wg := sync.WaitGroup{}
	newContext := context.Background()
	newContextWithValue := context.WithValue(newContext, log.ContextreqIDKey, reqIDValue)
	ctx, cancelFunc := context.WithCancel(newContextWithValue)
	rc.cancelFunc = cancelFunc

	for _, backend := range rc.Backends {
		wg.Add(1)
		go func(backend *StorageClient) {
			defer wg.Done()

			replicatedRequest, err := replicateRequest(ctx, request)
			if err != nil {
				responsesChan <- BackendResponse{Request: request.Request,
				Response: nil,
				Error: fmt.Errorf("failed to replicate request: %s", err),
				Backend: backend}
				return
			}
			isBRespSuccessful := callBackend(replicatedRequest, backend, responsesChan)
			if request.logRecord != nil {
				request.logRecord.AddBackendResult(isBRespSuccessful)
			}
		}(backend)
	}

	go func() {
		wg.Wait()
		defer close(responsesChan)
		if request.logRecord == nil {
			return
		}
		if request.logRecord.IsReflectedOnAllStorages() {
			log.Debugf("Request '%s' reflected on all storages", reqIDValue)
			err := rc.watchdog.Delete(request.marker)
			if err != nil {
				log.Printf("Failed to delete records for request %s: %s", reqIDValue, err.Error())
			}
		}
		log.Debugf("Request '%s' not reflected on all storages", reqIDValue)

	}()
	return responsesChan
}
func replicateRequest(ctx context.Context, request *Request) (*http.Request, error) {
	replicatedRequest, err := http.NewRequest(request.Method, request.URL.String(), request.Body)
	if resetter, ok := replicatedRequest.Body.(types.Resetter); ok {
		replicatedRequest.Body = resetter.Reset()
	}
	if err != nil {
		return nil, err
	}
	replicatedRequest.Header = http.Header{}
	for headerName, headerValues := range request.Header {
		for idx := range headerValues {
			replicatedRequest.Header.Add(headerName, headerValues[idx])
		}
	}
	return replicatedRequest.WithContext(ctx), nil
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

func callBackend(request *http.Request, backend *backend.Backend, backendResponseChan chan BackendResponse) bool {
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

	isBRespSuccessful := bresp.IsSuccessful()
	backendResponseChan <- bresp
	return isBRespSuccessful
}
