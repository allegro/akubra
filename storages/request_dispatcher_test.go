package storages

import (
	"context"
	"net/http"
	"testing"

	"github.com/allegro/akubra/regions/config"
	"github.com/allegro/akubra/storages/backend"
	"github.com/allegro/akubra/watchdog"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const (
	authHeaderV4 = "AWS4-HMAC-SHA256 Credential=TestKey/20181129/us-east-1/execute-api/aws4_request, SignedHeaders=host;x-amz-date, Signature=f0ae335103f009cd17f164fa2326efe18f79f0f3af941ea651e1ae2acc0326ea"
)

func TestRequestDispatcherPicks(t *testing.T) {
	matchReplicationClient := func(rep interface{}) bool {
		_, ok := rep.(*ReplicationClient)
		return ok
	}
	firstSuccessfulResponsePicker := func(pic interface{}) bool {
		_, ok := pic.(*ObjectResponsePicker)
		return ok
	}
	matchResponseMerger := func(pic interface{}) bool {
		_, ok := pic.(*responseMerger)
		return ok
	}
	allResponsesSuccessfulPicker := func(pic interface{}) bool {
		_, ok := pic.(*baseDeleteResponsePicker)
		return ok
	}
	multipartMultipartReplicator := func(rep interface{}) bool {
		_, ok := rep.(*MultiPartRoundTripper)
		return ok
	}
	testCases := []struct {
		method             string
		url                string
		expectedReplicator func(interface{}) bool
		expectedPicker     func(interface{}) bool
	}{
		{"GET", "http://some.storage/bucket/object", matchReplicationClient, firstSuccessfulResponsePicker},
		{"PUT", "http://some.storage/bucket/object", matchReplicationClient, firstSuccessfulResponsePicker},
		{"HEAD", "http://some.storage/bucket/object", matchReplicationClient, firstSuccessfulResponsePicker},
		{"DELETE", "http://some.storage/bucket/object", matchReplicationClient, firstSuccessfulResponsePicker},
		{"POST", "http://some.storage/bucket/object?uploads", multipartMultipartReplicator, firstSuccessfulResponsePicker},
		{"POST", "http://some.storage/bucket/object?uploadId=ssssss", multipartMultipartReplicator, firstSuccessfulResponsePicker},
		{"GET", "http://some.storage/bucket", matchReplicationClient, matchResponseMerger},
		{"HEAD", "http://some.storage/bucket", matchReplicationClient, firstSuccessfulResponsePicker},
		{"PUT", "http://some.storage/bucket", matchReplicationClient, allResponsesSuccessfulPicker},
	}

	dispatcher := NewRequestDispatcher(nil)
	require.NotNil(t, dispatcher)
	for _, tc := range testCases {
		request, _ := http.NewRequest(tc.method, tc.url, nil)
		replicatorFac := dispatcher.pickClientFactory(request)
		require.NotNil(t, replicatorFac)
		replicator := replicatorFac(nil)
		pickerFac := dispatcher.pickResponsePickerFactory(request)
		require.NotNil(t, pickerFac)
		pic := pickerFac(nil)
		require.True(t, tc.expectedReplicator(replicator))
		require.True(t, tc.expectedPicker(pic))
	}
}

func TestRequestDispatcherDispatch(t *testing.T) {
	dispatcher, clientMock, respPickerMock := prepareTest([]*backend.Backend{})
	require.NotNil(t, dispatcher)

	request, err := http.NewRequest("GET", "http://random.domain/bucket/object", nil)
	request = request.WithContext(context.WithValue(request.Context(), watchdog.Domain, "random.domain"))
	request = request.WithContext(context.WithValue(request.Context(), watchdog.ConsistencyLevel, config.None))
	request = request.WithContext(context.WithValue(request.Context(), watchdog.ReadRepair, false))

	require.NoError(t, err)
	require.NotNil(t, request)

	respChan := make(chan BackendResponse)
	response := &http.Response{}
	go func() { respChan <- BackendResponse{Response: response, Error: nil} }()
	require.NotNil(t, clientMock)

	clientMock.On("Do", request).Return(respChan)
	respPickerMock.On("Pick").Return(response, nil)
	_, _ = dispatcher.Dispatch(request)
	clientMock.AssertExpectations(t)
	respPickerMock.AssertExpectations(t)

}

func prepareTest(backends []*backend.Backend) (*RequestDispatcher, *replicationClientMock, *responsePickerMock) {
	respPickerMock := &responsePickerMock{&mock.Mock{}}
	responsePickerFactoryMock := responsePickFactoryMockFactory(respPickerMock)
	clientMock := &replicationClientMock{&mock.Mock{}}
	clientFactoryMock := clientFactoryMockFactory(clientMock)
	return &RequestDispatcher{
		Backends:                  backends,
		pickClientFactory:         clientFactoryMock,
		pickResponsePickerFactory: responsePickerFactoryMock,
	}, clientMock, respPickerMock

}

func clientFactoryMockFactory(mock *replicationClientMock) func(request *http.Request) func([]*StorageClient) client {
	return func(request *http.Request) func([]*StorageClient) client {
		return func([]*StorageClient) client {
			return mock
		}
	}
}

type replicationClientMock struct {
	*mock.Mock
}

func (rcm replicationClientMock) Do(req *http.Request) <-chan BackendResponse {
	args := rcm.Called(req)
	resp := args.Get(0).(chan BackendResponse)
	return resp
}

func (rcm replicationClientMock) Cancel() error {
	return nil
}

func responsePickFactoryMockFactory(mock *responsePickerMock) func(request *http.Request) func(<-chan BackendResponse) responsePicker {
	return func(request *http.Request) func(<-chan BackendResponse) responsePicker {
		return func(<-chan BackendResponse) responsePicker {
			return mock
		}
	}
}

type responsePickerMock struct {
	*mock.Mock
}

func (rpm *responsePickerMock) Pick() (*http.Response, error) {
	args := rpm.Called()
	httpResponse := args.Get(0).(*http.Response)
	err := args.Error(1)
	return httpResponse, err
}
