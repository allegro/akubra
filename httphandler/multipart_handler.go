package httphandler

import (
	"io/ioutil"
	"bytes"
	"encoding/xml"
	"time"
	"encoding/json"
	"strings"
	"net/http"
	"github.com/allegro/akubra/log"
	"github.com/allegro/akubra/types"
)

//MultiPartUploadHandler handles the multi part upload. If multi part upload is detected, it delegates the request
//to the backend selected by multipart_picker, otherwise the cluster round tripper is used
//to handle the operation in standard fashion
type MultiPartUploadHandler struct {
	multiPartUploadBackend  http.RoundTripper
	clusterRoundTripper     http.RoundTripper
	syncLog                 log.Logger
	backendsHostNamesToSync []string
}

//NewMultiPartUploadHandler constructs a new MultiPartUploadHandler and returns a pointer to it
func NewMultiPartUploadHandler(multiPartUploadBackend http.RoundTripper,
	clusterRoundTripper http.RoundTripper,
	syncLog log.Logger,
	backendsHostNamesToSync []string) *MultiPartUploadHandler {

	return &MultiPartUploadHandler{multiPartUploadBackend,
		clusterRoundTripper,
		syncLog,
		backendsHostNamesToSync}
}

//RoundTrip performs a RoundTrip using the strategy described in MultiPartUploadHandler
func (multiPartUploadHandler *MultiPartUploadHandler) RoundTrip(request *http.Request) (response *http.Response, err error) {

	if isMultiPartUploadRequest(request) {

		response, err = multiPartUploadHandler.multiPartUploadBackend.RoundTrip(request)

		if err == nil && !isInitiateRequest(request) && isCompleteUploadResponseSuccessful(response) {
			go multiPartUploadHandler.reportCompletionToMigrator(response)
		}

		return
	}

	return multiPartUploadHandler.clusterRoundTripper.RoundTrip(request)
}

func isMultiPartUploadRequest(request *http.Request) bool {
	return isInitiateRequest(request) || containsUploadID(request)
}

func isInitiateRequest(request *http.Request) bool {
	return strings.HasSuffix(request.URL.String(), "?uploads")
}

func containsUploadID(request *http.Request) bool {
	return strings.Contains(request.URL.RawQuery, "uploadId=")
}

func isCompleteUploadResponseSuccessful(response *http.Response) bool {
	return response.StatusCode == 200 &&
		!strings.Contains(response.Request.URL.RawQuery, "partNumber=") &&
		responseContainsCompleteUploadString(response)
}

func responseContainsCompleteUploadString(response *http.Response) bool {

	responseBodyBytes, bodyReadError := ioutil.ReadAll(response.Body)

	if bodyReadError != nil {

		log.Debugf(
			"Failed to read response body from CompleteMultipartUpload response for object %s, error: %s",
			response.Request.URL, bodyReadError)

		return false
	}

	response.Body = ioutil.NopCloser(bytes.NewBuffer(responseBodyBytes))

	var completeMultipartUploadResult types.CompleteMultipartUploadResult

	xmlParsingError := xml.Unmarshal(responseBodyBytes, &completeMultipartUploadResult)

	if xmlParsingError != nil {

		log.Debugf(
			"Failed to parse body from CompleteMultipartUpload response for %s, error: %s",
			response.Request.URL, xmlParsingError)

		return false
	}

	log.Debugf("Successfully performed multipart upload to %", completeMultipartUploadResult.Location)

	return true
}

func (multiPartUploadHandler *MultiPartUploadHandler) reportCompletionToMigrator(response *http.Response) {

	for _, backendHostName := range multiPartUploadHandler.backendsHostNamesToSync {

		syncLogMsg := &SyncLogMessageData{
			Method:        "PUT",
			FailedHost:    backendHostName,
			SuccessHost:   response.Request.Host,
			Path:          response.Request.URL.Path,
			AccessKey:     extractAccessKey(response.Request),
			UserAgent:     response.Request.Header.Get("User-Agent"),
			ContentLength: response.ContentLength,
			ErrorMsg:      "Migrate MultiUpload",
			ReqID:         requestID(response.Request),
			Time:          time.Now().Format(time.RFC3339Nano),
		}

		logMsg, err := json.Marshal(syncLogMsg)

		if err != nil {
			log.Debugf("Marshall synclog error %s", err)
			return
		}

		multiPartUploadHandler.syncLog.Println(string(logMsg))

		log.Debugf("Sent a multi part uplaod migration request of object %s to backend %s", response.Request.URL.Path, backendHostName)
	}
}