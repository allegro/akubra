package storages

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"net/url"
	"testing"

	"github.com/allegro/akubra/log"
	"github.com/serialx/hashring"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// import (
// 	"bytes"
// 	"context"
// 	"encoding/json"
// 	"fmt"
// 	"io/ioutil"
// 	"net/http"
// 	"sync"
// 	"testing"

// 	"net/url"

// 	"github.com/allegro/akubra/httphandler"
// 	"github.com/allegro/akubra/log"
// 	"github.com/serialx/hashring"
// 	"github.com/stretchr/testify/assert"
// 	"github.com/stretchr/testify/mock"
// )

//MOVED RESPONSIBILITY TO DISPATCHER
// func TestShouldNotDetectMultiPartUploadRequestWhenItIsARegularUpload(testSuite *testing.T) {

// 	requestURL, _ := url.Parse("http://localhost:3212/someBucket/someObject")
// 	notAMultiPartUploadRequest := &http.Request{URL: requestURL}
// 	responseFromFallBackRoundTripper := &http.Response{Request: notAMultiPartUploadRequest}

// 	syncLog := &MockedSyncLog{}
// 	fallbackRoundTripper := &MockedRoundTripper{}
// 	activeBackendRoundTripper := &MockedRoundTripper{}

// 	activeBackendURL, _ := url.Parse("http://active:1234")

// 	activateBackend := &Backend{
// 		RoundTripper: activeBackendRoundTripper,
// 		Endpoint:     *activeBackendURL,
// 		Maintenance:  false,
// 		Name:         "activateBackend",
// 	}

// 	multiPartUploadHashRing := hashring.New([]string{activeBackendURL.String()})
// 	activeBackendRoundTrippers := make(map[string]*Backend)
// 	activeBackendRoundTrippers[activateBackend.Endpoint.String()] = activateBackend

// 	multiPartRoundTripper := MultiPartRoundTripper{
// 		nil,
// 		activeBackendRoundTrippers,
// 		multiPartUploadHashRing,
// 		nil,
// 	}

// 	fallbackRoundTripper.On("RoundTrip", notAMultiPartUploadRequest).Return(responseFromFallBackRoundTripper, nil)

// 	response, err := multiPartRoundTripper.RoundTrip(notAMultiPartUploadRequest)

// 	assert.Equal(testSuite, response, responseFromFallBackRoundTripper)
// 	assert.Nil(testSuite, err)

// 	fallbackRoundTripper.AssertNumberOfCalls(testSuite, "RoundTrip", 1)
// 	activeBackendRoundTripper.AssertNumberOfCalls(testSuite, "RoundTrip", 0)
// 	syncLog.AssertNumberOfCalls(testSuite, "Println", 0)
// }

func TestShouldNotBeAbleToServeTheMultiPartUploadRequestWhenBackendRingIsEmpty(testSuite *testing.T) {

	requestURL, _ := url.Parse("http://localhost:3212/someBucket/someObject?uploads")
	multiPartUploadRequest := &http.Request{URL: requestURL}
	emptyMultiPartUploadHashRing := hashring.New([]string{})
	activeBackendRoundTrippers := make(map[string]*Backend)

	multiPartRoundTripper := MultiPartRoundTripper{
		nil,
		activeBackendRoundTrippers,
		emptyMultiPartUploadHashRing,
		nil,
	}

	respChan := multiPartRoundTripper.Do(multiPartUploadRequest)
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
		nil,
		make(map[string]*Backend),
		hashRingOnlyWithMaitenanceBackend,
		nil,
	}

	respChan := multiPartRoundTripper.Do(multiPartUploadRequest)
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
		nil,
		activeBackendRoundTrippers,
		multiPartUploadHashRing,
		[]string{activeBackendURL.String(), activeBackendURL2.String()},
	}

	activeBackendRoundTripper1.On("RoundTrip", initiateMultiPartUploadRequest).Return(responseForInitiate, nil)
	activeBackendRoundTripper1.On("RoundTrip", uploadPartRequest).Return(responseForPartUpload, nil)

	rChan1 := multiPartRoundTripper.Do(initiateMultiPartUploadRequest)
	for bresp := range rChan1 {
		assert.Equal(testSuite, bresp.Response, responseForInitiate)
		assert.NoError(testSuite, bresp.Error)
	}
	rChan2 := multiPartRoundTripper.Do(uploadPartRequest)
	for bresp := range rChan2 {
		assert.Equal(testSuite, bresp.Response, responseForPartUpload)
		assert.NoError(testSuite, bresp.Error)
	}

	activeBackendRoundTripper1.AssertNumberOfCalls(testSuite, "RoundTrip", 2)
	activeBackendRoundTripper2.AssertNumberOfCalls(testSuite, "RoundTrip", 0)
}

func TestShouldDetectMultiPartCompletionAndTryToNotifyTheMigratorButFailOnParsingTheResponse(testSuite *testing.T) {
	testBadResponse(200, "<InvalidResponse></InvalidResponse>", testSuite)
}

func TestShouldDetectMultiPartCompletionAndTryToNotifyTheMigratorWhenStatusCodeIsWrong(testSuite *testing.T) {
	testBadResponse(500, "<Error>Nope</Error>", testSuite)
}

func TestShouldDetectMultiPartCompletionAndSuccessfullyNotifyTheMigrator(testSuite *testing.T) {

	activeBackendURL1, _ := url.Parse("http://active:1234")
	inactiveBackendURL1, _ := url.Parse("http://active:1235")

	completeUploadRequestURL, _ := url.Parse("http://localhost:3212/someBucket/someObject?uploadId=123")
	completeUploadRequest := &http.Request{URL: completeUploadRequestURL, Host: activeBackendURL1.Host}
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

	activeBackendRoundTripper1 := &MockedRoundTripper{}

	activateBackend1 := &Backend{
		RoundTripper: activeBackendRoundTripper1,
		Endpoint:     *activeBackendURL1,
		Maintenance:  false,
		Name:         "activateBackend1",
	}

	inactivateBackend1 := &Backend{
		RoundTripper: activeBackendRoundTripper1,
		Endpoint:     *inactiveBackendURL1,
		Maintenance:  true,
		Name:         "activateBackend2",
	}

	multiPartUploadHashRing := hashring.New([]string{activateBackend1.Endpoint.String()})

	backendRoundTrippers := map[string]*Backend{
		inactivateBackend1.Endpoint.String(): inactivateBackend1,
		activateBackend1.Endpoint.String():   activateBackend1,
	}

	hostToSync := "hostToSync"
	hostToSync2 := "hostToSync2"

	multiPartRoundTripper := MultiPartRoundTripper{
		nil,
		backendRoundTrippers,
		multiPartUploadHashRing,
		[]string{activeBackendURL1.Host, hostToSync, hostToSync2},
	}

	activeBackendRoundTripper1.On("RoundTrip", completeUploadRequest).Return(responseForComplete, nil)

	rChan := multiPartRoundTripper.Do(completeUploadRequest)
	bresp := <-rChan
	assert.Equal(testSuite, bresp.Response, responseForComplete)
	activeBackendRoundTripper1.AssertNumberOfCalls(testSuite, "RoundTrip", 1)
	bresp = <-rChan
	require.Nil(testSuite, bresp.Response)
	require.Equal(testSuite, inactivateBackend1, bresp.Backend)
}

func testBadResponse(statusCode int, xmlResponse string, testSuite *testing.T) {

	completeUploadRequestURL, _ := url.Parse("http://localhost:3212/someBucket/someObject?uploadId=321")
	completeUploadRequest := &http.Request{URL: completeUploadRequestURL}

	responseForComplete := &http.Response{Request: completeUploadRequest}
	invalidXMLResponse := xmlResponse
	responseForComplete.Body = ioutil.NopCloser(bytes.NewBufferString(invalidXMLResponse))
	responseForComplete.StatusCode = 500

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

	multiPartRoundTripper := MultiPartRoundTripper{
		nil,
		activeBackendRoundTrippers,
		multiPartUploadHashRing,
		[]string{activeBackendURL.String(), activeBackendURL2.String()},
	}

	activeBackendRoundTripper1.On("RoundTrip", completeUploadRequest).Return(responseForComplete, nil)

	rChan := multiPartRoundTripper.Do(completeUploadRequest)
	for bresp := range rChan {
		assert.Equal(testSuite, bresp.Response, responseForComplete)
	}
	activeBackendRoundTripper1.AssertNumberOfCalls(testSuite, "RoundTrip", 1)
}
