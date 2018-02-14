package httphandler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"
	"testing"

	"github.com/allegro/akubra/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"golang.org/x/net/context"
)

type MockedRoundTripper struct {
	mock.Mock
	http.RoundTripper
}

type MockedSyncLog struct {
	mock.Mock
	log.Logger
}

func (mockedRoundTripper *MockedRoundTripper) RoundTrip(request *http.Request) (response *http.Response, err error) {

	args := mockedRoundTripper.Called(request)

	return args.Get(0).(*http.Response), args.Error(1)
}

func (syncLog *MockedSyncLog) Println(v ...interface{}) {
	syncLog.Called(v)
}

func TestShouldNotDetectMultiPartUploadRequestWhenItIsARegularUpload(testSuite *testing.T) {

	requestURL, _ := url.Parse("http://localhost:3212/someBucket/someObject")
	notAMultiPartUploadRequest := &http.Request{URL: requestURL}
	expectedResponse := &http.Response{Request: notAMultiPartUploadRequest}

	multiPartUploadBackend := &MockedRoundTripper{}
	cluster := &MockedRoundTripper{}
	syncLog := &MockedSyncLog{}

	multiPartUploadHandler := MultiPartUploadHandler{
		multiPartUploadBackend:  multiPartUploadBackend,
		clusterRoundTripper:     cluster,
		syncLog:                 syncLog,
		backendsHostNamesToSync: []string{},
	}

	cluster.On("RoundTrip", notAMultiPartUploadRequest).Return(expectedResponse, nil)

	response, err := multiPartUploadHandler.RoundTrip(notAMultiPartUploadRequest)

	assert.Equal(testSuite, response, expectedResponse)
	assert.Nil(testSuite, err)

	cluster.AssertNumberOfCalls(testSuite, "RoundTrip", 1)
	multiPartUploadBackend.AssertNumberOfCalls(testSuite, "RoundTrip", 0)
	syncLog.AssertNumberOfCalls(testSuite, "Println", 0)
}

func TestShouldDetectMultiPartUploadRequestWhenItIsAInitiateRequest(testSuite *testing.T) {

	requestURL, _ := url.Parse("http://localhost:3212/someBucket/someObject?uploads")
	initiateMultiPartUploadRequest := &http.Request{URL: requestURL}
	expectedResponse := &http.Response{Request: initiateMultiPartUploadRequest}

	multiPartUploadBackend := &MockedRoundTripper{}
	cluster := &MockedRoundTripper{}
	syncLog := &MockedSyncLog{}

	multiPartUploadHandler := MultiPartUploadHandler{
		multiPartUploadBackend:  multiPartUploadBackend,
		clusterRoundTripper:     cluster,
		syncLog:                 syncLog,
		backendsHostNamesToSync: []string{},
	}

	multiPartUploadBackend.On("RoundTrip", initiateMultiPartUploadRequest).Return(expectedResponse, nil)

	response, err := multiPartUploadHandler.RoundTrip(initiateMultiPartUploadRequest)

	assert.Equal(testSuite, response, expectedResponse)
	assert.Nil(testSuite, err)

	cluster.AssertNumberOfCalls(testSuite, "RoundTrip", 0)
	multiPartUploadBackend.AssertNumberOfCalls(testSuite, "RoundTrip", 1)
	syncLog.AssertNumberOfCalls(testSuite, "Println", 0)

}

func TestShouldDetectMultiPartUploadRequestWhenItContainsUploadIdInQuery(testSuite *testing.T) {

	requestURL, _ := url.Parse("http://localhost:3212/someBucket/someObject?uploadId=321&someOtherParam=abc")
	multiPartUploadRequest := &http.Request{URL: requestURL}
	expectedResponse := &http.Response{Request: multiPartUploadRequest}

	multiPartUploadBackend := &MockedRoundTripper{}
	cluster := &MockedRoundTripper{}
	syncLog := &MockedSyncLog{}

	multiPartUploadHandler := MultiPartUploadHandler{
		multiPartUploadBackend:  multiPartUploadBackend,
		clusterRoundTripper:     cluster,
		syncLog:                 syncLog,
		backendsHostNamesToSync: []string{},
	}

	multiPartUploadBackend.On("RoundTrip", multiPartUploadRequest).Return(expectedResponse, nil)

	response, err := multiPartUploadHandler.RoundTrip(multiPartUploadRequest)

	assert.Equal(testSuite, response, expectedResponse)
	assert.Nil(testSuite, err)

	cluster.AssertNumberOfCalls(testSuite, "RoundTrip", 0)
	multiPartUploadBackend.AssertNumberOfCalls(testSuite, "RoundTrip", 1)
	syncLog.AssertNumberOfCalls(testSuite, "Println", 0)
}

func TestShouldDetectMultiPartCompletionAndTryToNotifyTheMigratorButFailOnParsingTheResponse(testSuite *testing.T) {

	requestURL, _ := url.Parse("http://localhost:3212/someBucket/someObject?uploadId=321")
	completeMultiPartUploadRequest := &http.Request{URL: requestURL}
	expectedResponse := &http.Response{Request: completeMultiPartUploadRequest}

	invalidXMLResponse := "<rootNode><subnode>test</subnode></rootNode>"
	expectedResponse.StatusCode = 200
	expectedResponse.Body = ioutil.NopCloser(bytes.NewBufferString(invalidXMLResponse))

	multiPartUploadBackend := &MockedRoundTripper{}
	cluster := &MockedRoundTripper{}
	syncLog := &MockedSyncLog{}

	multiPartUploadHandler := MultiPartUploadHandler{
		multiPartUploadBackend:  multiPartUploadBackend,
		clusterRoundTripper:     cluster,
		syncLog:                 syncLog,
		backendsHostNamesToSync: []string{},
	}

	multiPartUploadBackend.On("RoundTrip", completeMultiPartUploadRequest).Return(expectedResponse, nil)

	response, err := multiPartUploadHandler.RoundTrip(completeMultiPartUploadRequest)

	assert.Equal(testSuite, response, expectedResponse)
	assert.Nil(testSuite, err)

	cluster.AssertNumberOfCalls(testSuite, "RoundTrip", 0)
	multiPartUploadBackend.AssertNumberOfCalls(testSuite, "RoundTrip", 1)
	syncLog.AssertNumberOfCalls(testSuite, "Println", 0)
}

func TestShouldDetectMultiPartCompletionAndNotNotifyMigratorWhenStatusCodeIsWrong(testSuite *testing.T) {

	requestURL, _ := url.Parse("http://localhost:3212/someBucket/someObject?uploadId=321")
	completeMultiPartUploadRequest := &http.Request{URL: requestURL}
	expectedResponse := &http.Response{Request: completeMultiPartUploadRequest}

	errorResponse := "<Error>nope</Error>"
	expectedResponse.StatusCode = 500
	expectedResponse.Body = ioutil.NopCloser(bytes.NewBufferString(errorResponse))

	multiPartUploadBackend := &MockedRoundTripper{}
	cluster := &MockedRoundTripper{}
	syncLog := &MockedSyncLog{}

	multiPartUploadHandler := MultiPartUploadHandler{
		multiPartUploadBackend:  multiPartUploadBackend,
		clusterRoundTripper:     cluster,
		syncLog:                 syncLog,
		backendsHostNamesToSync: []string{},
	}

	multiPartUploadBackend.On("RoundTrip", completeMultiPartUploadRequest).Return(expectedResponse, nil)

	response, err := multiPartUploadHandler.RoundTrip(completeMultiPartUploadRequest)

	assert.Equal(testSuite, response, expectedResponse)
	assert.Nil(testSuite, err)

	cluster.AssertNumberOfCalls(testSuite, "RoundTrip", 0)
	multiPartUploadBackend.AssertNumberOfCalls(testSuite, "RoundTrip", 1)
	syncLog.AssertNumberOfCalls(testSuite, "Println", 0)
}

func TestShouldDetectMultiPartCompletionAndSuccessfullyNotifyTheMigrator(testSuite *testing.T) {

	requestURL, _ := url.Parse("http://localhost:3212/someBucket/someObject?uploadId=321")
	completeMultiPartUploadRequest := &http.Request{URL: requestURL}
	completeMultiPartUploadRequest = completeMultiPartUploadRequest.WithContext(context.WithValue(context.Background(), log.ContextreqIDKey, "1"))

	expectedResponse := &http.Response{Request: completeMultiPartUploadRequest}

	validXMLResponse := "<CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
		"<Location>http://locahost:9092/someBucket/someBucket</Location>" +
		"<Bucket>someBucket</Bucket>" +
		"<Key>someBucket</Key>" +
		"<ETag>\"3858f62230ac3c915f300c664312c11f-9\"</ETag>" +
		"</CompleteMultipartUploadResult>"

	expectedResponse.StatusCode = 200
	expectedResponse.Body = ioutil.NopCloser(bytes.NewBufferString(validXMLResponse))

	var notificationWaitGroup sync.WaitGroup
	notificationWaitGroup.Add(2)

	multiPartUploadBackend := &MockedRoundTripper{}
	cluster := &MockedRoundTripper{}
	syncLog := &MockedSyncLog{}

	multiPartUploadHandler := MultiPartUploadHandler{
		multiPartUploadBackend:  multiPartUploadBackend,
		clusterRoundTripper:     cluster,
		syncLog:                 syncLog,
		backendsHostNamesToSync: []string{"host1", "host2"},
	}

	multiPartUploadBackend.On("RoundTrip", completeMultiPartUploadRequest).Return(expectedResponse, nil)
	syncLog.On("Println", mock.AnythingOfType("[]interface {}")).Run(func(args mock.Arguments) {

		syncRequestJSON, _ := args.Get(0).([]interface{})[0].(string)

		var syncRequest SyncLogMessageData
		err := json.Unmarshal([]byte(syncRequestJSON), &syncRequest)

		if err != nil {
			panic(fmt.Sprintf("Failed to unmarshall the response - %s", err))
		}

		if syncRequest.Path == "/someBucket/someObject" && (syncRequest.FailedHost == "host1" || syncRequest.FailedHost == "host2") {
			notificationWaitGroup.Done()
		} else {
			panic("Wrong host name in syncRequest")
		}
	})

	response, err := multiPartUploadHandler.RoundTrip(completeMultiPartUploadRequest)

	notificationWaitGroup.Wait()

	assert.Equal(testSuite, response, expectedResponse)
	assert.Nil(testSuite, err)

	cluster.AssertNumberOfCalls(testSuite, "RoundTrip", 0)
	multiPartUploadBackend.AssertNumberOfCalls(testSuite, "RoundTrip", 1)
	syncLog.AssertNumberOfCalls(testSuite, "Println", 2)
}