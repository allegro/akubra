package storages

import (
	"context"
	"fmt"
	"github.com/allegro/akubra/utils"
	"github.com/allegro/akubra/watchdog"
	"net/http"
	"sync"

	"github.com/allegro/akubra/log"
	"github.com/allegro/akubra/storages/backend"
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
	reqIDValue, ok := request.Context().Value(log.ContextreqIDKey).(string)
	if !ok {
		reqIDValue = ""
	}
	responsesChan := make(chan BackendResponse)
	wg := sync.WaitGroup{}
	newContext := context.Background()

	replicationContext := context.WithValue(newContext, log.ContextreqIDKey, reqIDValue)
	replicationContext, cancelFunc := context.WithCancel(replicationContext)
	rc.cancelFunc = cancelFunc

	allBackendsSucces := true
	mx := sync.Mutex{}
	for _, backend := range rc.Backends {
		wg.Add(1)
		go func(backend *StorageClient) {
			defer wg.Done()
			replicatedRequest, err := utils.ReplicateRequest(request.WithContext(replicationContext))
			if err != nil {
				responsesChan <- BackendResponse{Request: request,
					Response: nil,
					Error:    fmt.Errorf("failed to replicate request: %s", err),
					Backend:  backend}
				return
			}
			bRespSuccessfull := callBackend(replicatedRequest, backend, responsesChan)
			mx.Lock()
			defer mx.Unlock()
			allBackendsSucces = allBackendsSucces && bRespSuccessfull
		}(backend)
	}

	go func() {
		ctx := request.Context()
		wg.Wait()
		close(responsesChan)
		noErrors, ok := ctx.Value(watchdog.NoErrorsDuringRequest).(*bool)
		if ok && noErrors != nil {
			*noErrors = *noErrors && allBackendsSucces
		}
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
