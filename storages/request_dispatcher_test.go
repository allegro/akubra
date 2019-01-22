package storages

import (
	"context"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	"github.com/allegro/akubra/log"
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

	dispatcher := NewRequestDispatcher(nil, nil, nil)
	require.NotNil(t, dispatcher)
	for _, tc := range testCases {
		request, _ := http.NewRequest(tc.method, tc.url, nil)
		replicatorFac := dispatcher.pickClientFactory(request)
		require.NotNil(t, replicatorFac)
		replicator := replicatorFac(nil, nil)
		pickerFac := dispatcher.pickResponsePickerFactory(&Request{Request: request})
		require.NotNil(t, pickerFac)
		pic := pickerFac(nil, nil, nil)
		require.True(t, tc.expectedReplicator(replicator))
		require.True(t, tc.expectedPicker(pic))
	}
}

func TestRequestDispatcherDispatch(t *testing.T) {
	dispatcher, clientMock, respPickerMock, _, _ := prepareTest([]*backend.Backend{})
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

	clientMock.On("Do", &Request{Request: request}).Return(respChan)
	respPickerMock.On("Pick").Return(response, nil)
	dispatcher.Dispatch(request)
	clientMock.AssertExpectations(t)
	respPickerMock.AssertExpectations(t)

}

type dispatcherRequestScenario struct {
	method                  string
	url                     string
	backends                []*backend.Backend
	consistencyLevel        config.ConsistencyLevel
	readRepair 				bool
	shouldInsertFail        bool
	multiPartUploadID       string
	respBody                io.ReadCloser
}

func TestAddingConsistencyRecords(t *testing.T) {
	var requestScenarios = [] dispatcherRequestScenario{
		{"PUT", "http://random.domain/bucket/object", []*backend.Backend{}, config.None, false, false, "", nil},
		{"GET", "http://random.domain/bucket/object", []*backend.Backend{{}}, config.None, false, false, "", nil},
		{"GET", "http://random.domain/bucket", []*backend.Backend{{}}, config.None, false, false, "", nil},
		{"PUT", "http://random.domain/bucket", []*backend.Backend{{}}, config.None, false, false, "", nil},
		{"PUT", "http://random.domain/bucket/object", []*backend.Backend{{}, {}}, config.Strong, false, false, "", nil},
		{"PUT", "http://random.domain/bucket/object", []*backend.Backend{{}, {}}, config.Strong,false, true, "", nil},
		{"POST", "http://random.domain/bucket/object?uploads", []*backend.Backend{{}, {}}, config.Strong, false, false, "123", ioutil.NopCloser(strings.NewReader(initiateMultiPartResp))},
		{"POST", "http://random.domain/bucket/object?uploads", []*backend.Backend{{}, {}}, config.Strong, false, true, "123", ioutil.NopCloser(strings.NewReader(initiateMultiPartResp))},
		{"PUT", "http://random.domain/bucket/object", []*backend.Backend{{}, {}}, config.Weak,false, true, "", nil},
	}

	for _, requestScenario := range requestScenarios {
		dispatcher, clientMock, respPickerMock, watchdogMock, watchdogRecordFactory := prepareTest(requestScenario.backends)
		require.NotNil(t, clientMock)

		storageRequest, response := prepareTestScenario(t, &requestScenario, watchdogMock, watchdogRecordFactory, clientMock, respPickerMock)

		dispatcher.Dispatch(storageRequest.Request)
		clientMock.AssertExpectations(t)
		respPickerMock.AssertExpectations(t)

		assertTestScenario(t, storageRequest, response, &requestScenario, watchdogMock, respPickerMock)
	}
}
func prepareTestScenario(t *testing.T,
	requestScenario *dispatcherRequestScenario,
	watchdogMock *WatchdogMock,
	watchdogRecordFactory *ConsistencyRecordFactoryMock,
	clientMock *replicationClientMock,
	respPickerMock *responsePickerMock) (*Request, *http.Response) {

	isMultiPart := strings.Contains(requestScenario.url, "?uploads")

	request, err := http.NewRequest(requestScenario.method, requestScenario.url, nil)
	require.NoError(t, err)
	require.NotNil(t, request)
	request.Header.Add("Authorization", authHeaderV4)

	reqWithContext := supplyRequestWithIDAndDomain(request, requestScenario)
	record := &watchdog.ConsistencyRecord{}
	watchdogRecordFactory.On("CreateRecordFor", reqWithContext).Return(record, nil)
	watchdogMock.On("GetVersionHeaderName").Return("x-amz-meta-version")

	respChan := make(chan BackendResponse)
	response := &http.Response{}
	response.Body = requestScenario.respBody

	go func() { respChan <- BackendResponse{Response: response, Error: nil} }()
	require.NotNil(t, clientMock)
	storageRequest := &Request{Request: reqWithContext, logRecord: record}

	if requestScenario.consistencyLevel != config.None {
		if requestScenario.shouldInsertFail {
			if requestScenario.consistencyLevel != config.Strong {
				clientMock.On("Do", storageRequest).Return(respChan)
			}
			if isMultiPart {
				watchdogMock.On("SupplyRecordWithVersion", record).Return(errors.New("db error"))
			} else {
				watchdogMock.On("Insert", record).Return(nil, errors.New("db error"))
			}

		} else {
			marker := &watchdog.DeleteMarker{}
			if isMultiPart {
				storageRequest.isInitiateMultipartUploadRequest = true
				storageRequest.isMultiPartUploadRequest = true
				watchdogMock.On("SupplyRecordWithVersion", record).Return(nil)
				watchdogMock.On("InsertWithRequestID", requestScenario.multiPartUploadID, record).Return(marker, nil)
			} else {
				storageRequest.marker = marker
				watchdogMock.On("Insert", record).Return(marker, nil)
			}
			clientMock.On("Do", storageRequest).Return(respChan)
		}
	} else {
		clientMock.On("Do", &Request{Request: reqWithContext}).Return(respChan)
	}

	if !requestScenario.shouldInsertFail || requestScenario.consistencyLevel != config.Strong {
		respPickerMock.On("Pick").Return(response, nil)
	}

	return storageRequest, response
}

func assertTestScenario(
	t *testing.T,
	storageRequest *Request, response *http.Response,
	requestScenario *dispatcherRequestScenario,
	watchdogMock *WatchdogMock,
	respPickerMock *responsePickerMock) {
	isMultiPart := strings.Contains(requestScenario.url, "?uploads")

	if requestScenario.consistencyLevel != config.None {

		if !isMultiPart {
			watchdogMock.AssertCalled(t, "Insert", storageRequest.logRecord)
		}
		if requestScenario.shouldInsertFail && requestScenario.consistencyLevel == config.Strong {
			respPickerMock.AssertNotCalled(t, "Pick", response)
			if isMultiPart && requestScenario.multiPartUploadID != "" {
				watchdogMock.AssertCalled(t, "SupplyRecordWithVersion", storageRequest.logRecord)
				watchdogMock.AssertNotCalled(t, "InsertWithRequestID", requestScenario.multiPartUploadID, storageRequest.logRecord)
			}
		} else {
			if isMultiPart && requestScenario.multiPartUploadID != "" {
				watchdogMock.AssertCalled(t, "InsertWithRequestID", requestScenario.multiPartUploadID, storageRequest.logRecord)
			}
		}

	} else {

		watchdogMock.AssertNotCalled(t, "Insert", storageRequest.logRecord)
		watchdogMock.AssertNotCalled(t, "InsertWithRequestID", requestScenario.multiPartUploadID, storageRequest.logRecord)
		respPickerMock.AssertNotCalled(t, "Pick", response)

	}
}

func supplyRequestWithIDAndDomain(request *http.Request, scenario *dispatcherRequestScenario) *http.Request {
	recordedReqContext := context.WithValue(request.Context(), log.ContextreqIDKey, "testID")
	recordedReqContext = context.WithValue(recordedReqContext, watchdog.Domain, "testCluster")
	recordedReqContext = context.WithValue(recordedReqContext, watchdog.ConsistencyLevel, scenario.consistencyLevel)
	recordedReqContext = context.WithValue(recordedReqContext, watchdog.ReadRepair, scenario.readRepair)
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

func responsePickFactoryMockFactory(mock *responsePickerMock) func(request *Request) func(<-chan BackendResponse, watchdog.ConsistencyWatchdog, *watchdog.ConsistencyRecord) responsePicker {
	return func(request *Request) func(<-chan BackendResponse, watchdog.ConsistencyWatchdog, *watchdog.ConsistencyRecord) responsePicker {
		return func(<-chan BackendResponse, watchdog.ConsistencyWatchdog, *watchdog.ConsistencyRecord) responsePicker {
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

func (wm *WatchdogMock) InsertWithRequestID(requestID string, record *watchdog.ConsistencyRecord) (*watchdog.DeleteMarker, error) {
	args := wm.Called(requestID, record)
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

func (wm *WatchdogMock) UpdateExecutionDelay(delta *watchdog.ExecutionDelay) error {
	args := wm.Called(delta)
	return args.Error(0)
}

func (wm *WatchdogMock) SupplyRecordWithVersion(record *watchdog.ConsistencyRecord) (error) {
	args := wm.Called(record)
	return args.Error(0)
}

func (wm *WatchdogMock) GetVersionHeaderName() string {
	wm.Called()
	return "x-amz-meta-version"
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

const initiateMultiPartResp = "<InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
	"<Bucket>example-bucket</Bucket>" +
	"<Key>example-object</Key>" +
	"<UploadID>123</UploadID>" +
	"</InitiateMultipartUploadResult>"
