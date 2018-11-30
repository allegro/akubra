package storages

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/allegro/akubra/log"
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
	matchObjectResponsePicker := func(pic interface{}) bool {
		_, ok := pic.(*ObjectResponsePicker)
		return ok
	}
	matchResponseMerger := func(pic interface{}) bool {
		_, ok := pic.(*responseMerger)
		return ok
	}
	matchDeletePicker := func(pic interface{}) bool {
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

	dispatcher := NewRequestDispatcher(nil, nil, nil, nil)
	require.NotNil(t, dispatcher)
	for _, tc := range testCases {
		request, _ := http.NewRequest(tc.method, tc.url, nil)
		replicatorFac := dispatcher.pickClientFactory(request)
		require.NotNil(t, replicatorFac)
		replicator := replicatorFac(nil, nil)
		pickerFac := dispatcher.pickResponsePickerFactory(&Request{request, nil, nil})
		require.NotNil(t, pickerFac)
		pic := pickerFac(nil)
		require.True(t, tc.expectedReplicator(replicator))
		require.True(t, tc.expectedPicker(pic))
	}
}

func TestRequestDispatcherDispatch(t *testing.T) {
	dispatcher, clientMock, respPickerMock, _, _ := prepareTest([]*backend.Backend{})
	require.NotNil(t, dispatcher)

	request, err := http.NewRequest("GET", "http://random.domain/bucket/object", nil)
	require.NoError(t, err)
	require.NotNil(t, request)

	respChan := make(chan BackendResponse)
	response := &http.Response{}
	go func() { respChan <- BackendResponse{Response: response, Error: nil} }()
	require.NotNil(t, clientMock)
	clientMock.On("Do", &Request{request, nil, nil}).Return(respChan)
	respPickerMock.On("Pick").Return(response, nil)
	dispatcher.Dispatch(request)
	clientMock.AssertExpectations(t)
	respPickerMock.AssertExpectations(t)

}

func TestAddingConsistencyRecords(t *testing.T) {
	var requestScenarios = [] struct {
		method                 string
		url                    string
		backends               []*backend.Backend
		shouldTryToInsertRecord bool
		shouldInsertFail       bool
	}{
		{"PUT", "http://random.domain/bucket/object", []*backend.Backend{}, false, false},
		{"GET", "http://random.domain/bucket/object", []*backend.Backend{{}}, false, false},
		{"GET", "http://random.domain/bucket", []*backend.Backend{{}}, false, false},
		{"PUT", "http://random.domain/bucket", []*backend.Backend{{}}, false, false},
		{"PUT", "http://random.domain/bucket/object", []*backend.Backend{{}, {}}, true, false},
		{"PUT", "http://random.domain/bucket/object", []*backend.Backend{{}, {}}, true, true},
	}

	for _, requestScenario := range requestScenarios {
		dispatcher, clientMock, respPickerMock, watchdogMock, watchdogRecordFactory := prepareTest(requestScenario.backends)

		request, err := http.NewRequest(requestScenario.method, requestScenario.url, nil)
		require.NoError(t, err)
		require.NotNil(t, request)
		request.Header.Add("Authorization", authHeaderV4)

		reqWithContext := supplyRequestWithIDAndClusterName(request)
		record := &watchdog.ConsistencyRecord{}
		watchdogRecordFactory.On("CreateRecordFor", reqWithContext).Return(record, nil)

		respChan := make(chan BackendResponse)
		response := &http.Response{}
		go func() { respChan <- BackendResponse{Response: response, Error: nil} }()
		require.NotNil(t, clientMock)

		if requestScenario.shouldTryToInsertRecord {
			if requestScenario.shouldInsertFail {
				watchdogMock.On("Insert", record).Return(nil, errors.New("db error"))
			} else {
				marker := &watchdog.DeleteMarker{}
				watchdogMock.On("Insert", record).Return(marker, nil)
				clientMock.On("Do", &Request{reqWithContext, record, marker}).Return(respChan)
			}
		} else {
			clientMock.On("Do", &Request{reqWithContext, nil, nil}).Return(respChan)
		}

		if !requestScenario.shouldInsertFail {
			respPickerMock.On("Pick").Return(response, nil)
		}

		dispatcher.Dispatch(reqWithContext)
		clientMock.AssertExpectations(t)
		respPickerMock.AssertExpectations(t)

		if !requestScenario.shouldInsertFail {
			respPickerMock.AssertNotCalled(t, "Pick", response)
		}

		if requestScenario.shouldTryToInsertRecord {
			watchdogMock.AssertCalled(t, "Insert", record)
		} else {
			watchdogMock.AssertNotCalled(t, "Insert", record)
		}
	}
}

func supplyRequestWithIDAndClusterName(request *http.Request) *http.Request {
	recordedReqContext := context.WithValue(request.Context(), log.ContextreqIDKey, "testID")
	recordedReqContext = context.WithValue(recordedReqContext, watchdog.ClusterName, "testCluster")
	return request.WithContext(recordedReqContext)
}

func prepareTest(backends []*backend.Backend) (*RequestDispatcher, *replicationClientMock, *responsePickerMock, *WatchdogMock, *ConsistencyRecordFactoryMock) {
	respPickerMock := &responsePickerMock{&mock.Mock{}}
	responsePickerFactoryMock := responsePickFactoryMockFactory(respPickerMock)
	clientMock := &replicationClientMock{&mock.Mock{}}
	clientFactoryMock := clientFactoryMockFactory(clientMock)
	watchdogMock := &WatchdogMock{&mock.Mock{}}
	recordFactory := &ConsistencyRecordFactoryMock{&mock.Mock{}}
	return &RequestDispatcher{
		Backends:                  backends,
		pickClientFactory:         clientFactoryMock,
		pickResponsePickerFactory: responsePickerFactoryMock,
		watchdog:                  watchdogMock,
		watchdogRecordFactory:     recordFactory,
	}, clientMock, respPickerMock, watchdogMock, recordFactory

}

func clientFactoryMockFactory(mock *replicationClientMock) func(request *http.Request) func([]*StorageClient, watchdog.ConsistencyWatchdog) client {
	return func(request *http.Request) func([]*StorageClient, watchdog.ConsistencyWatchdog) client {
		return func([]*StorageClient, watchdog.ConsistencyWatchdog) client {
			return mock
		}
	}
}

type replicationClientMock struct {
	*mock.Mock
}

func (rcm replicationClientMock) Do(req *Request) <-chan BackendResponse {
	args := rcm.Called(req)
	resp := args.Get(0).(chan BackendResponse)
	return resp
}

func (rcm replicationClientMock) Cancel() error {
	return nil
}

func responsePickFactoryMockFactory(mock *responsePickerMock) func(request *Request) func(<-chan BackendResponse) responsePicker {
	return func(request *Request) func(<-chan BackendResponse) responsePicker {
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

type WatchdogMock struct {
	*mock.Mock
}

func (wm *WatchdogMock) Insert(record *watchdog.ConsistencyRecord) (*watchdog.DeleteMarker, error) {
	args := wm.Called(record)
	arg0 := args.Get(0)
	var deleteMarker *watchdog.DeleteMarker
	if arg0 != nil {
		deleteMarker = arg0.(*watchdog.DeleteMarker)
	}
	err := args.Error(1)
	return deleteMarker, err
}

func (wm *WatchdogMock) Delete(marker *watchdog.DeleteMarker) error {
	args := wm.Called(marker)
	return args.Error(0)
}

func (wm *WatchdogMock) UpdateExecutionTime(delta *watchdog.ExecutionTimeDelta) error {
	args := wm.Called(delta)
	return args.Error(0)
}

type ConsistencyRecordFactoryMock struct {
	*mock.Mock
}

func (fm *ConsistencyRecordFactoryMock) CreateRecordFor(request *http.Request) (*watchdog.ConsistencyRecord, error) {
	args := fm.Called(request)
	record := args.Get(0).(*watchdog.ConsistencyRecord)
	err := args.Error(1)
	return record, err
}
