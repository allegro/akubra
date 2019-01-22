package storages

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"net/url"
	"testing"

	"github.com/allegro/akubra/watchdog"
	"github.com/serialx/hashring"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestShouldNotBeAbleToServeTheMultiPartUploadRequestWhenBackendRingIsEmpty(testSuite *testing.T) {

	requestURL, _ := url.Parse("http://localhost:3212/someBucket/someObject?uploads")
	multiPartUploadRequest := &http.Request{URL: requestURL}
	emptyMultiPartUploadHashRing := hashring.New([]string{})
	activeBackendRoundTrippers := make(map[string]*StorageClient)

	multiPartRoundTripper := MultiPartRoundTripper{
		activeBackendRoundTrippers,
		emptyMultiPartUploadHashRing,
		nil,
		nil,
	}

	respChan := multiPartRoundTripper.Do(&Request{Request: multiPartUploadRequest})
	for resp := range respChan {
		assert.Error(testSuite, resp.Error, "can't handle multi upload")
	}
}

func TestShouldNotBeAbleToServeTheMultiPartUploadRequestWhenAllBackendsAreInMaintenance(testSuite *testing.T) {

	requestURL, _ := url.Parse("http://localhost:3212/someBucket/someObject?uploads")
	multiPartUploadRequest := &http.Request{URL: requestURL}

	maintenanceBackendURL, _ := url.Parse("http://maintenance:8421")
	hashRingOnlyWithMaitenanceBackend := hashring.New([]string{maintenanceBackendURL.String()})

	multiPartRoundTripper := MultiPartRoundTripper{
		make(map[string]*StorageClient),
		hashRingOnlyWithMaitenanceBackend,
		nil,
		nil,
	}

	respChan := multiPartRoundTripper.Do(&Request{Request: multiPartUploadRequest})
	for resp := range respChan {
		assert.Error(testSuite, resp.Error, "can't handle multi upload")
	}
}

func TestShouldDetectMultiPartUploadRequestWhenItIsAInitiateRequestOrUploadPartRequest(testSuite *testing.T) {

	initiateRequestURL, _ := url.Parse("http://localhost:3212/someBucket/someObject?uploads")
	initiateMultiPartUploadRequest := &http.Request{URL: initiateRequestURL}

	uploadPartRequestURL, _ := url.Parse("http://localhost:3212/someBucket/someObject?partNumber=1&uploadId=123")
	uploadPartRequest := &http.Request{URL: uploadPartRequestURL}

	responseForInitiate := &http.Response{Request: initiateMultiPartUploadRequest}
	responseForPartUpload := &http.Response{Request: uploadPartRequest}

	activeBackendRoundTripper1 := &MockedRoundTripper{}
	activeBackendRoundTripper2 := &MockedRoundTripper{}

	activeBackendURL, _ := url.Parse("http://active:1234")
	activeBackendURL2, _ := url.Parse("http://active2:1234")

	activateBackend1 := &StorageClient{
		RoundTripper: activeBackendRoundTripper1,
		Endpoint:     *activeBackendURL,
		Maintenance:  false,
		Name:         "activateBackend",
	}

	activateBackend2 := &StorageClient{
		RoundTripper: activeBackendRoundTripper2,
		Endpoint:     *activeBackendURL2,
		Maintenance:  false,
		Name:         "activateBackend2",
	}

	multiPartUploadHashRing := hashring.New([]string{activateBackend1.Endpoint.String(), activateBackend2.Endpoint.String()})

	activeBackendRoundTrippers := make(map[string]*StorageClient)
	activeBackendRoundTrippers[activateBackend1.Endpoint.String()] = activateBackend1
	activeBackendRoundTrippers[activateBackend2.Endpoint.String()] = activateBackend2

	multiPartRoundTripper := MultiPartRoundTripper{
		activeBackendRoundTrippers,
		multiPartUploadHashRing,
		[]string{activeBackendURL.String(), activeBackendURL2.String()},
		nil,
	}

	activeBackendRoundTripper1.On("RoundTrip", initiateMultiPartUploadRequest).Return(responseForInitiate, nil)
	activeBackendRoundTripper1.On("RoundTrip", uploadPartRequest).Return(responseForPartUpload, nil)

	rChan1 := multiPartRoundTripper.Do(&Request{Request: initiateMultiPartUploadRequest})
	for bresp := range rChan1 {
		assert.Equal(testSuite, bresp.Response, responseForInitiate)
		assert.NoError(testSuite, bresp.Error)
	}
	rChan2 := multiPartRoundTripper.Do(&Request{Request: uploadPartRequest})
	for bresp := range rChan2 {
		assert.Equal(testSuite, bresp.Response, responseForPartUpload)
		assert.NoError(testSuite, bresp.Error)
	}

	activeBackendRoundTripper1.AssertNumberOfCalls(testSuite, "RoundTrip", 2)
	activeBackendRoundTripper2.AssertNumberOfCalls(testSuite, "RoundTrip", 0)
}

func TestShouldDetectMultiPartCompletionAndTryToNotifyTheMigratorButFailOnParsingTheResponse(testSuite *testing.T) {
	testMultipartFlow(200, "<InvalidResponse></InvalidResponse>", nil, nil, nil, nil, testSuite)
}

func TestShouldDetectMultiPartCompletionAndTryToNotifyTheMigratorWhenStatusCodeIsWrong(testSuite *testing.T) {
	testMultipartFlow(500, "<Error>Nope</Error>", nil, nil, nil, nil, testSuite)
}

func TestShouldUpdateExecutionDelayOfTheConsistencyRecordIfMultiPartWasSuccessful(t *testing.T) {
	testMultipartFlow(200, successfulMultipartResponse, &watchdog.ConsistencyRecord{}, &watchdog.ExecutionDelay{RequestID: "321", Delay: 300000000000}, &watchdog.DeleteMarker{}, &WatchdogMock{&mock.Mock{}}, t)
}

func TestShouldNotUpdateExecutionTimeIfWatchdogIsNotDefined(t *testing.T) {
	testMultipartFlow(200, successfulMultipartResponse, nil, nil, nil, nil, t)
}

func testMultipartFlow(statusCode int, xmlResponse string,
	record *watchdog.ConsistencyRecord, delta *watchdog.ExecutionDelay, marker *watchdog.DeleteMarker,
	watchdogMock *WatchdogMock, testSuite *testing.T) {

	completeUploadRequestURL, _ := url.Parse("http://localhost:3212/someBucket/someObject?uploadId=321")
	completeUploadRequest := &http.Request{URL: completeUploadRequestURL}
	completeUploadRequest = completeUploadRequest.WithContext(context.WithValue(completeUploadRequest.Context(), watchdog.Domain, "321"))

	responseForComplete := &http.Response{Request: completeUploadRequest}
	XMLResponse := xmlResponse
	responseForComplete.Body = ioutil.NopCloser(bytes.NewBufferString(XMLResponse))
	responseForComplete.StatusCode = statusCode

	activeBackendRoundTripper1 := &MockedRoundTripper{}
	activeBackendRoundTripper2 := &MockedRoundTripper{}

	activeBackendURL, _ := url.Parse("http://active:1234")

	activateBackend1 := &StorageClient{
		RoundTripper: activeBackendRoundTripper1,
		Endpoint:     *activeBackendURL,
		Maintenance:  false,
		Name:         "activateBackend1",
	}

	activeBackendURL2, _ := url.Parse("http://active2:1234")

	activateBackend2 := &StorageClient{
		RoundTripper: activeBackendRoundTripper2,
		Endpoint:     *activeBackendURL2,
		Maintenance:  false,
		Name:         "activateBackend2",
	}

	multiPartUploadHashRing := hashring.New([]string{activateBackend1.Endpoint.String(), activateBackend2.Endpoint.String()})

	activeBackendRoundTrippers := make(map[string]*StorageClient)
	activeBackendRoundTrippers[activateBackend1.Endpoint.String()] = activateBackend1
	activeBackendRoundTrippers[activateBackend2.Endpoint.String()] = activateBackend2

	multiPartRoundTripper := MultiPartRoundTripper{
		activeBackendRoundTrippers,
		multiPartUploadHashRing,
		[]string{activeBackendURL.String(), activeBackendURL2.String()},
		nil,
	}

	if watchdogMock != nil && delta != nil {
		multiPartRoundTripper.watchdog = watchdogMock
		watchdogMock.On("UpdateExecutionDelay", delta).Return(nil)
	}

	activeBackendRoundTripper1.On("RoundTrip", completeUploadRequest).Return(responseForComplete, nil)

	rChan := multiPartRoundTripper.Do(&Request{Request: completeUploadRequest, logRecord: record, marker: marker})
	for bresp := range rChan {
		if bresp.Response == nil {
			continue
		}
		assert.Equal(testSuite, bresp.Response, responseForComplete)
	}

	if watchdogMock != nil {
		watchdogMock.AssertCalled(testSuite, "UpdateExecutionDelay", delta)
	}

	activeBackendRoundTripper1.AssertNumberOfCalls(testSuite, "RoundTrip", 1)
}

const successfulMultipartResponse = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
	"<CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
	"<Location>http://Example-Bucket.s3.amazonaws.com/Example-Object</Location>" +
	"<Bucket>Example-Bucket</Bucket>" +
	"<Key>Example-Object</Key>" +
	"<ETag>\"3858f62230ac3c915f300c664312c11f-9\"</ETag>" +
	"</CompleteMultipartUploadResult>"
