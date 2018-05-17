package storages

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sync"

	"github.com/allegro/akubra/log"
)

// ErrRequestCanceled is returned if request was canceled
var ErrRequestCanceled = fmt.Errorf("Request canceled")

// ReplicationClient is multiple endpoints client
type ReplicationClient struct {
	Backends   []*Backend
	cancelFunc context.CancelFunc
}

// NewReplicationClient returns ReplicarionClient
func NewReplicationClient(backends []*Backend) *ReplicationClient {
	return &ReplicationClient{Backends: backends}
}

// Do send request to all given backends
func (rc *ReplicationClient) Do(request *http.Request) <-chan BackendResponse {
	responsesChan := make(chan BackendResponse)
	wg := sync.WaitGroup{}
	ctx, cancelFunc := context.WithCancel(request.Context())
	rc.cancelFunc = cancelFunc
	for _, backend := range rc.Backends {
		wg.Add(1)
		go func(backend *Backend) {
			requestWithContext := request.WithContext(ctx)
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
	if rc.cancelFunc == nil {
		return fmt.Errorf("No operation in progress cannot cancel")
	}
	rc.cancelFunc()
	return nil
}

// BackendResponse helps handle responses
type BackendResponse struct {
	Response *http.Response
	Error    error
	Backend  *Backend
}

// DiscardBody drain and close response Body, so connections are properly closed
func (br *BackendResponse) DiscardBody() {
	if br.Response == nil || br.Response.Body == nil {
		return
	}
	_, err := io.Copy(ioutil.Discard, br.Response.Body)
	if err != nil {
		log.Printf("Discard body error %s", err)
	}
	err = br.Response.Body.Close()
}

func callBackend(request *http.Request, backend *Backend, backendResponseChan chan BackendResponse) {
	resp, err := backend.RoundTrip(request)
	contextErr := request.Context().Err()
	if contextErr != nil {
		err = ErrRequestCanceled
	}
	backendResponseChan <- BackendResponse{Response: resp, Error: err, Backend: backend}
}
