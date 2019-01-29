package storages

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/allegro/akubra/log"
	"github.com/allegro/akubra/storages/backend"
	"github.com/allegro/akubra/watchdog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestReplicationClientCreation(t *testing.T) {
	backends := []*StorageClient{}
	cli := newReplicationClient(backends, nil)
	require.NotNil(t, cli)
}

func TestReplicationClientRequestPassing(t *testing.T) {
	callCount := 0
	callClountHandler := func(*http.Request) (*http.Response, error) {
		callCount++
		return nil, nil
	}

	backends := []*StorageClient{createDummyBackend(callClountHandler)}
	cli := newReplicationClient(backends, nil)
	require.NotNil(t, cli)

	request := dummyRequest()
	responses := cli.Do(&Request{Request: request})

	responsesCount := 0
	for range responses {
		responsesCount++
	}

	require.Equal(t, len(backends), callCount, "Not all backends called")
	require.Equal(t, len(backends), responsesCount, "Not all responses passed")
}

func TestWatchdogIntegration(t *testing.T) {
	var watchdogRequestScenarios = []struct {
		numOfBackends      int
		shouldDeleteRecord bool
		failedBackendIndex int
	}{
		{2, true, -1},
		{2, false, 0},
	}
	for _, requestScenario := range watchdogRequestScenarios {
		request := createRequest(t, "PUT", "http://random.domain/bucket/object", "testCluster", "123")
		record, err := (&watchdog.DefaultConsistencyRecordFactory{}).CreateRecordFor(request)
		assert.NotNil(t, record)
		assert.NoError(t, err)

		deleteMarker := &watchdog.DeleteMarker{}

		watchdogMock := &WatchdogMock{&mock.Mock{}}
		watchdogMock.On("Delete", deleteMarker).Return(nil)

		alwaysSuccessfulHandler := func(r *http.Request) (*http.Response, error) {
			return &http.Response{Request: r, StatusCode: http.StatusOK}, nil
		}
		alwaysFailingHandler := func(r *http.Request) (*http.Response, error) {
			return nil, errors.New("network err")
		}

		var backends []*backend.Backend
		for i := 0; i < requestScenario.numOfBackends; i++ {
			if i == requestScenario.failedBackendIndex {
				backends = append(backends, createDummyBackend(alwaysFailingHandler))
			} else {
				backends = append(backends, createDummyBackend(alwaysSuccessfulHandler))
			}
		}

		cli := newReplicationClient(backends, watchdogMock)
		respChan := cli.Do(&Request{Request: request, logRecord: record, marker: deleteMarker})

		for range respChan {
		}

		if requestScenario.shouldDeleteRecord {
			watchdogMock.AssertCalled(t, "Delete", deleteMarker)
		} else {
			watchdogMock.AssertNotCalled(t, "Delete", deleteMarker)
		}
	}
}

func createRequest(t *testing.T, method string, reqURL string, clusterName string, reqID string) *http.Request {
	rURL, err := url.Parse(reqURL)
	assert.NoError(t, err)
	assert.NotNil(t, rURL)

	req := &http.Request{
		Method: method,
		URL:    rURL,
	}
	req.Header = http.Header{}
	req.Header.Add("Authorization", authHeaderV4)
	req = req.WithContext(context.WithValue(context.Background(), watchdog.Domain, clusterName))
	return req.WithContext(context.WithValue(req.Context(), log.ContextreqIDKey, reqID))
}

func TestHttpCancelContext(t *testing.T) {
	handlerf := http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		time.Sleep(20 * time.Second)
	})
	srv := httptest.NewServer(handlerf)
	cancelCtx, cancelFunc := context.WithCancel(context.Background())

	request, err := http.NewRequest("GET", srv.URL, nil)
	require.NoError(t, err)

	requestWithCtx := request.WithContext(cancelCtx)

	cli := srv.Client()

	errChan := make(chan error)
	start := time.Now()
	go func() {
		_, err = cli.Do(requestWithCtx)
		errChan <- err
	}()

	cancelFunc()

	err = <-errChan
	require.Error(t, err)
	require.True(t, time.Since(start) < 20*time.Second, "Should endup almost immediately")

}

func TestReplicationClientCancelRequest(t *testing.T) {
	backends := []*StorageClient{createDummyBackend(slowRoundTripper), createDummyBackend(successRoundTripper)}

	cli := newReplicationClient(backends, nil)
	request := dummyRequest()

	responses := cli.Do(&Request{Request: request})

	cancelCount := 0
	for resp := range responses {
		err := cli.Cancel()
		require.NoError(t, err, "Cancel should not return error once Do method is called")
		if resp.Error == ErrRequestCanceled {
			cancelCount++
		}
	}
	require.Equal(t, 1, cancelCount, "At least one request should be canceled")

}

func dummyRequest() *http.Request {
	request, _ := http.NewRequest("GET", "http://example.com", nil)
	return request
}

type RequestHandler func(*http.Request) (*http.Response, error)

var slowRoundTripper = func(*http.Request) (*http.Response, error) {
	time.Sleep(20 * time.Millisecond)
	return nil, fmt.Errorf("Connection timeout")
}

var successRoundTripper = func(req *http.Request) (*http.Response, error) {
	return &http.Response{Request: req}, nil
}

func createDummyBackend(handler RequestHandler) *StorageClient {
	url, _ := url.Parse("http://some.url")
	return &StorageClient{Endpoint: *url, RoundTripper: &testRt{rt: handler}}
}

type testRt struct {
	rt func(*http.Request) (*http.Response, error)
}

func (trt *testRt) RoundTrip(req *http.Request) (*http.Response, error) {
	return trt.rt(req)
}
