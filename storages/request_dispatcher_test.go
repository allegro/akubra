package storages

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestRequestDispatcherPicks(t *testing.T) {
	matchReplicationClient := func(rep interface{}) bool {
		_, ok := rep.(*ReplicationClient)
		return ok
	}
	matchObjectResponsePicker := func(pic interface{}) bool {
		_, ok := pic.(*ObjectResponsePicker)
		return ok
	}
	matchResponseMerger := func(pic interface{}) bool {
		_, ok := pic.(*responseMerger)
		return ok
	}
	matchDeletePicker := func(pic interface{}) bool {
		_, ok := pic.(*deleteResponsePicker)
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
		{"GET", "http://some.storage/bucket/object", matchReplicationClient, matchObjectResponsePicker},
		{"PUT", "http://some.storage/bucket/object", matchReplicationClient, matchObjectResponsePicker},
		{"HEAD", "http://some.storage/bucket/object", matchReplicationClient, matchObjectResponsePicker},
		{"DELETE", "http://some.storage/bucket/object", matchReplicationClient, matchDeletePicker},
		{"POST", "http://some.storage/bucket/object?uploads", multipartMultipartReplicator, matchObjectResponsePicker},
		{"POST", "http://some.storage/bucket/object?uploadId=ssssss", multipartMultipartReplicator, matchObjectResponsePicker},
		{"GET", "http://some.storage/bucket", matchReplicationClient, matchResponseMerger},
		{"HEAD", "http://some.storage/bucket", matchReplicationClient, matchObjectResponsePicker},
		{"PUT", "http://some.storage/bucket", matchReplicationClient, matchDeletePicker},
	}

	dispatcher := NewRequestDispatcher(nil, nil)
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
	respPickerMock := &responsePickerMock{&mock.Mock{}}
	responsePickerFactoryMock := responsePickFactoryMockFactory(respPickerMock)
	clientMock := &replicationClientMock{&mock.Mock{}}
	clientFactoryMock := clientFactoryMockFactory(clientMock)
	dispatcher := &RequestDispatcher{
		Backends:                  nil,
		pickClientFactory:         clientFactoryMock,
		pickResponsePickerFactory: responsePickerFactoryMock,
	}
	require.NotNil(t, dispatcher)

	request, err := http.NewRequest("GET", "http://random.domain/bucket/object", nil)
	require.NoError(t, err)
	require.NotNil(t, request)

	respChan := make(chan BackendResponse)
	response := &http.Response{}
	go func() { respChan <- BackendResponse{Response: response, Error: nil} }()
	require.NotNil(t, clientMock)
	clientMock.On("Do", request).Return(respChan)
	respPickerMock.On("Pick").Return(response, nil)
	dispatcher.Dispatch(request)
	clientMock.AssertExpectations(t)
	respPickerMock.AssertExpectations(t)

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
func (*responsePickerMock) SendSyncLog(*SyncSender) {}
