package storages

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"testing"

	"net/url"

	"github.com/allegro/akubra/httphandler"
	"github.com/allegro/akubra/log"
	"github.com/serialx/hashring"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockedSyncLog struct {
	mock.Mock
	log.Logger
}

func (syncLog *MockedSyncLog) Println(v ...interface{}) {
	syncLog.Called(v)
}

func TestShouldNotDetectMultiPartUploadRequestWhenItIsARegularUpload(testSuite *testing.T) {

	requestURL, _ := url.Parse("http://localhost:3212/someBucket/someObject")
	notAMultiPartUploadRequest := &http.Request{URL: requestURL}
	responseFromFallBackRoundTripper := &http.Response{Request: notAMultiPartUploadRequest}

	syncLog := &MockedSyncLog{}
	fallbackRoundTripper := &MockedRoundTripper{}
	activeBackendRoundTripper := &MockedRoundTripper{}

	activeBackendURL, _ := url.Parse("http://active:1234")

	activateBackend := &Backend{
		RoundTripper: activeBackendRoundTripper,
		Endpoint:     *activeBackendURL,
		Maintenance:  false,
		Name:         "activateBackend",
	}

	multiPartUploadHashRing := hashring.New([]string{activeBackendURL.String()})
	activeBackendRoundTrippers := make(map[string]*Backend)
	activeBackendRoundTrippers[activateBackend.Endpoint.String()] = activateBackend

	multiPartRoundTripper := MultiPartRoundTripper{
		fallbackRoundTripper,
		nil,
		activeBackendRoundTrippers,
		multiPartUploadHashRing,
		nil,
	}

	fallbackRoundTripper.On("RoundTrip", notAMultiPartUploadRequest).Return(responseFromFallBackRoundTripper, nil)

	response, err := multiPartRoundTripper.RoundTrip(notAMultiPartUploadRequest)

	assert.Equal(testSuite, response, responseFromFallBackRoundTripper)
	assert.Nil(testSuite, err)

	fallbackRoundTripper.AssertNumberOfCalls(testSuite, "RoundTrip", 1)
	activeBackendRoundTripper.AssertNumberOfCalls(testSuite, "RoundTrip", 0)
	syncLog.AssertNumberOfCalls(testSuite, "Println", 0)
}

func TestShouldNotBeAbleToServeTheMultiPartUploadRequestWhenBackendRingIsEmpty(testSuite *testing.T) {

	requestURL, _ := url.Parse("http://localhost:3212/someBucket/someObject?uploads")
	multiPartUploadRequest := &http.Request{URL: requestURL}
	responseFromFallBackRoundTripper := &http.Response{Request: multiPartUploadRequest}

	syncLog := &MockedSyncLog{}
	fallbackRoundTripper := &MockedRoundTripper{}

	emptyMultiPartUploadHashRing := hashring.New([]string{})
	activeBackendRoundTrippers := make(map[string]*Backend)

	multiPartRoundTripper := MultiPartRoundTripper{
		fallbackRoundTripper,
		nil,
		activeBackendRoundTrippers,
		emptyMultiPartUploadHashRing,
		nil,
	}

	fallbackRoundTripper.On("RoundTrip", multiPartUploadRequest).Return(responseFromFallBackRoundTripper, nil)

	_, err := multiPartRoundTripper.RoundTrip(multiPartUploadRequest)

	assert.Error(testSuite, err, "can't handle multi upload")
	fallbackRoundTripper.AssertNumberOfCalls(testSuite, "RoundTrip", 0)
	syncLog.AssertNumberOfCalls(testSuite, "Println", 0)
}

func TestShouldNotBeAbleToServeTheMultiPartUploadRequestWhenAllBackendsAreInMaintenance(testSuite *testing.T) {

	requestURL, _ := url.Parse("http://localhost:3212/someBucket/someObject?uploads")
	multiPartUploadRequest := &http.Request{URL: requestURL}
	responseFromFallBackRoundTripper := &http.Response{Request: multiPartUploadRequest}

	syncLog := &MockedSyncLog{}
	fallbackRoundTripper := &MockedRoundTripper{}

	maintenanceBackendURL, _ := url.Parse("http://maintenance:8421")
	hashRingOnlyWithMaitenanceBackend := hashring.New([]string{maintenanceBackendURL.String()})

	multiPartRoundTripper := MultiPartRoundTripper{
		fallbackRoundTripper,
		nil,
		make(map[string]*Backend),
		hashRingOnlyWithMaitenanceBackend,
		nil,
	}

	fallbackRoundTripper.On("RoundTrip", multiPartUploadRequest).Return(responseFromFallBackRoundTripper, nil)

	_, err := multiPartRoundTripper.RoundTrip(multiPartUploadRequest)

	assert.Error(testSuite, err, "can't handle multi upload")
	fallbackRoundTripper.AssertNumberOfCalls(testSuite, "RoundTrip", 0)
	syncLog.AssertNumberOfCalls(testSuite, "Println", 0)
}

func TestShouldDetectMultiPartUploadRequestWhenItIsAInitiateRequestOrUploadPartRequest(testSuite *testing.T) {

	initiateRequestURL, _ := url.Parse("http://localhost:3212/someBucket/someObject?uploads")
	initiateMultiPartUploadRequest := &http.Request{URL: initiateRequestURL}

	uploadPartRequestURL, _ := url.Parse("http://localhost:3212/someBucket/someObject?partNumber=1&uploadId=123")
	uploadPartRequest := &http.Request{URL: uploadPartRequestURL}

	responseForInitiate := &http.Response{Request: initiateMultiPartUploadRequest}
	responseForPartUpload := &http.Response{Request: uploadPartRequest}

	syncLog := &MockedSyncLog{}
	fallbackRoundTripper := &MockedRoundTripper{}
	activeBackendRoundTripper1 := &MockedRoundTripper{}
	activeBackendRoundTripper2 := &MockedRoundTripper{}

	activeBackendURL, _ := url.Parse("http://active:1234")
	activeBackendURL2, _ := url.Parse("http://active2:1234")

	activateBackend1 := &Backend{
		RoundTripper: activeBackendRoundTripper1,
		Endpoint:     *activeBackendURL,
		Maintenance:  false,
		Name:         "activateBackend",
	}

	activateBackend2 := &Backend{
		RoundTripper: activeBackendRoundTripper2,
		Endpoint:     *activeBackendURL2,
		Maintenance:  false,
		Name:         "activateBackend2",
	}

	multiPartUploadHashRing := hashring.New([]string{activateBackend1.Endpoint.String(), activateBackend2.Endpoint.String()})

	activeBackendRoundTrippers := make(map[string]*Backend)
	activeBackendRoundTrippers[activateBackend1.Endpoint.String()] = activateBackend1
	activeBackendRoundTrippers[activateBackend2.Endpoint.String()] = activateBackend2

	multiPartRoundTripper := MultiPartRoundTripper{
		fallbackRoundTripper,
		nil,
		activeBackendRoundTrippers,
		multiPartUploadHashRing,
		[]string{activeBackendURL.String(), activeBackendURL2.String()},
	}

	activeBackendRoundTripper1.On("RoundTrip", initiateMultiPartUploadRequest).Return(responseForInitiate, nil)
	activeBackendRoundTripper1.On("RoundTrip", uploadPartRequest).Return(responseForPartUpload, nil)

	akubraResponseForInitiateRequest, err1 := multiPartRoundTripper.RoundTrip(initiateMultiPartUploadRequest)
	akubraResponseForPartUploadRequest, err2 := multiPartRoundTripper.RoundTrip(uploadPartRequest)

	assert.Equal(testSuite, akubraResponseForInitiateRequest, responseForInitiate)
	assert.Equal(testSuite, akubraResponseForPartUploadRequest, responseForPartUpload)

	assert.Nil(testSuite, err1)
	assert.Nil(testSuite, err2)

	fallbackRoundTripper.AssertNumberOfCalls(testSuite, "RoundTrip", 0)
	activeBackendRoundTripper1.AssertNumberOfCalls(testSuite, "RoundTrip", 2)
	activeBackendRoundTripper2.AssertNumberOfCalls(testSuite, "RoundTrip", 0)
	syncLog.AssertNumberOfCalls(testSuite, "Println", 0)
}

func TestShouldDetectMultiPartCompletionAndTryToNotifyTheMigratorButFailOnParsingTheResponse(testSuite *testing.T) {
	testBadResponse(200, "<InvalidResponse></InvalidResponse>", testSuite)
}

func TestShouldDetectMultiPartCompletionAndTryToNotifyTheMigratorWhenStatusCodeIsWrong(testSuite *testing.T) {
	testBadResponse(500, "<Error>Nope</Error>", testSuite)
}

func TestShouldDetectMultiPartCompletionAndSuccessfullyNotifyTheMigrator(testSuite *testing.T) {

	completeUploadRequestURL, _ := url.Parse("http://localhost:3212/someBucket/someObject?uploadId=123")
	completeUploadRequest := &http.Request{URL: completeUploadRequestURL}
	completeUploadRequest = completeUploadRequest.WithContext(context.WithValue(context.Background(), log.ContextreqIDKey, "1"))

	responseForComplete := &http.Response{Request: completeUploadRequest}

	validXMLResponse := "<CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
		"<Location>http://locahost:9092/someBucket/someBucket</Location>" +
		"<Bucket>someBucket</Bucket>" +
		"<Key>someBucket</Key>" +
		"<ETag>\"3858f62230ac3c915f300c664312c11f-9\"</ETag>" +
		"</CompleteMultipartUploadResult>"

	responseForComplete.StatusCode = 200
	responseForComplete.Body = ioutil.NopCloser(bytes.NewBufferString(validXMLResponse))

	syncLog := &MockedSyncLog{}
	fallbackRoundTripper := &MockedRoundTripper{}
	activeBackendRoundTripper1 := &MockedRoundTripper{}

	activeBackendURL1, _ := url.Parse("http://active:1234")
	activateBackend1 := &Backend{
		RoundTripper: activeBackendRoundTripper1,
		Endpoint:     *activeBackendURL1,
		Maintenance:  false,
		Name:         "activateBackend1",
	}

	multiPartUploadHashRing := hashring.New([]string{activateBackend1.Endpoint.String()})

	activeBackendRoundTrippers := make(map[string]*Backend)
	activeBackendRoundTrippers[activateBackend1.Endpoint.String()] = activateBackend1

	hostToSync := "hostToSync"
	hostToSync2 := "hostToSync2"

	var notificationWaitGroup sync.WaitGroup
	notificationWaitGroup.Add(2)

	syncLog.On("Println", mock.AnythingOfType("[]interface {}")).Run(func(args mock.Arguments) {

		syncRequestJSON, _ := args.Get(0).([]interface{})[0].(string)

		var syncRequest httphandler.SyncLogMessageData
		err := json.Unmarshal([]byte(syncRequestJSON), &syncRequest)

		if err != nil {
			panic(fmt.Sprintf("Failed to unmarshall the response - %s", err))
		}

		if syncRequest.Path == "/someBucket/someObject" && (syncRequest.FailedHost == hostToSync || syncRequest.FailedHost == hostToSync2) {
			notificationWaitGroup.Done()
		} else {
			panic("Wrong host name in syncRequest")
		}
	})

	multiPartRoundTripper := MultiPartRoundTripper{
		fallbackRoundTripper,
		syncLog,
		activeBackendRoundTrippers,
		multiPartUploadHashRing,
		[]string{activeBackendURL1.String(), hostToSync, hostToSync2},
	}

	activeBackendRoundTripper1.On("RoundTrip", completeUploadRequest).Return(responseForComplete, nil)

	akubraResponseForCompleteRequest, _ := multiPartRoundTripper.RoundTrip(completeUploadRequest)

	notificationWaitGroup.Wait()
	assert.Equal(testSuite, akubraResponseForCompleteRequest, responseForComplete)
	fallbackRoundTripper.AssertNumberOfCalls(testSuite, "RoundTrip", 0)
	activeBackendRoundTripper1.AssertNumberOfCalls(testSuite, "RoundTrip", 1)
	syncLog.AssertNumberOfCalls(testSuite, "Println", 2)
}

func testBadResponse(statusCode int, xmlResponse string, testSuite *testing.T) {

	completeUploadRequestURL, _ := url.Parse("http://localhost:3212/someBucket/someObject?uploadId=321")
	completeUploadRequest := &http.Request{URL: completeUploadRequestURL}

	responseForComplete := &http.Response{Request: completeUploadRequest}
	invalidXMLResponse := xmlResponse
	responseForComplete.Body = ioutil.NopCloser(bytes.NewBufferString(invalidXMLResponse))
	responseForComplete.StatusCode = 500

	fallbackRoundTripper := &MockedRoundTripper{}
	activeBackendRoundTripper1 := &MockedRoundTripper{}
	activeBackendRoundTripper2 := &MockedRoundTripper{}

	activeBackendURL, _ := url.Parse("http://active:1234")
	activateBackend1 := &Backend{
		RoundTripper: activeBackendRoundTripper1,
		Endpoint:     *activeBackendURL,
		Maintenance:  false,
		Name:         "activateBackend1",
	}

	activeBackendURL2, _ := url.Parse("http://active2:1234")
	activateBackend2 := &Backend{
		RoundTripper: activeBackendRoundTripper2,
		Endpoint:     *activeBackendURL2,
		Maintenance:  false,
		Name:         "activateBackend2",
	}

	multiPartUploadHashRing := hashring.New([]string{activateBackend1.Endpoint.String(), activateBackend2.Endpoint.String()})

	activeBackendRoundTrippers := make(map[string]*Backend)
	activeBackendRoundTrippers[activateBackend1.Endpoint.String()] = activateBackend1
	activeBackendRoundTrippers[activateBackend2.Endpoint.String()] = activateBackend2

	syncLog := &MockedSyncLog{}

	multiPartRoundTripper := MultiPartRoundTripper{
		fallbackRoundTripper,
		syncLog,
		activeBackendRoundTrippers,
		multiPartUploadHashRing,
		[]string{activeBackendURL.String(), activeBackendURL2.String()},
	}

	activeBackendRoundTripper1.On("RoundTrip", completeUploadRequest).Return(responseForComplete, nil)

	akubraResponseForCompleteRequest, _ := multiPartRoundTripper.RoundTrip(completeUploadRequest)

	assert.Equal(testSuite, akubraResponseForCompleteRequest, responseForComplete)
	fallbackRoundTripper.AssertNumberOfCalls(testSuite, "RoundTrip", 0)
	activeBackendRoundTripper1.AssertNumberOfCalls(testSuite, "RoundTrip", 1)
	syncLog.AssertNumberOfCalls(testSuite, "Println", 0)
}
