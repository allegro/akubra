package storages

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"errors"

	"github.com/allegro/akubra/httphandler"
	"github.com/allegro/akubra/log"
	"github.com/allegro/akubra/types"
	"github.com/allegro/akubra/utils"
	"github.com/serialx/hashring"
)

//MultiPartRoundTripper handles the multi part upload. If multi part upload is detected, it delegates the request
//to the backend selected by multipart_picker, otherwise the cluster round tripper is used
//to handle the operation in standard fashion
type MultiPartRoundTripper struct {
	fallBackRoundTripper  http.RoundTripper
	syncLog               log.Logger
	backendsRoundTrippers map[string]http.RoundTripper
	activeBackendsRing    *hashring.HashRing
	hostsToSync           []string
}

//NewMultiPartRoundTripper constructs a new MultiPartRoundTripper and returns a pointer to it
func NewMultiPartRoundTripper(cluster *Cluster, syncLog log.Logger) *MultiPartRoundTripper {

	multiPartRoundTripper := &MultiPartRoundTripper{
		fallBackRoundTripper: cluster.transport,
		syncLog:              syncLog,
	}

	multiPartRoundTripper.setupRoundTripper(cluster.Backends())
	return multiPartRoundTripper
}

//MultiPartUploadUploadRing handles multi part uploads
type MultiPartUploadUploadRing struct {
	backendsRoundTrippers map[string]http.RoundTripper
	activeBackendsRing    *hashring.HashRing
	hostsToSync           []string
}

func (multiPartRoundTripper *MultiPartRoundTripper) setupRoundTripper(backends []http.RoundTripper) {

	multiPartRoundTripper.hostsToSync = []string{}
	multiPartRoundTripper.backendsRoundTrippers = make(map[string]http.RoundTripper)

	var activeBackendsNames []string

	for _, roundTripper := range backends {

		if backend, isBackendType := roundTripper.(*Backend); isBackendType {

			if backend.Maintenance {

				multiPartRoundTripper.hostsToSync = append(multiPartRoundTripper.hostsToSync, backend.Endpoint.String())

			} else {

				multiPartRoundTripper.backendsRoundTrippers[backend.Name] = backend
				activeBackendsNames = append(activeBackendsNames, backend.Name)
			}
		}
	}

	multiPartRoundTripper.activeBackendsRing = hashring.New(activeBackendsNames)
}

//RoundTrip performs a RoundTrip using the strategy described in MultiPartRoundTripper
func (multiPartRoundTripper *MultiPartRoundTripper) RoundTrip(request *http.Request) (response *http.Response, requestError error) {

	if isMultiPartUploadRequest(request) {

		if multiPartRoundTripper.isNotAbleToHandleMultiUpload() {
			return nil, errors.New("can't handle multi upload")
		}

		log.Debugf("Handling multi part upload, for object %s with id %s", request.URL.Path, request.Context().Value(log.ContextreqIDKey))

		multiUploadBackendName, _ := multiPartRoundTripper.activeBackendsRing.GetNode(request.URL.Path)
		multiUploadBackend := multiPartRoundTripper.backendsRoundTrippers[multiUploadBackendName]

		response, requestError = multiUploadBackend.RoundTrip(request)

		if requestError != nil {
			log.Debugf("Error during multi part upload: %s", requestError)
			return
		}

		if !isInitiateRequest(request) && isCompleteUploadResponseSuccessful(response) {
			go multiPartRoundTripper.reportCompletionToMigrator(response)
		}

		log.Debugf("Served multi part request, response code %d, status %s", response.StatusCode, response.Status)

		return
	}

	return multiPartRoundTripper.fallBackRoundTripper.RoundTrip(request)
}

func (multiPartRoundTripper *MultiPartRoundTripper) isNotAbleToHandleMultiUpload() bool {
	return multiPartRoundTripper.activeBackendsRing.Size() < 1
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

	log.Debugf("Successfully performed multipart upload to %s", completeMultipartUploadResult.Location)

	return true
}

func (multiPartRoundTripper *MultiPartRoundTripper) reportCompletionToMigrator(response *http.Response) {

	for _, backendHostName := range multiPartRoundTripper.hostsToSync {

		syncLogMsg := &httphandler.SyncLogMessageData{
			Method:        "PUT",
			FailedHost:    backendHostName,
			SuccessHost:   response.Request.Host,
			Path:          response.Request.URL.Path,
			AccessKey:     utils.ExtractAccessKey(response.Request),
			UserAgent:     response.Request.Header.Get("User-Agent"),
			ContentLength: response.ContentLength,
			ErrorMsg:      "Migrate MultiUpload",
			ReqID:         utils.RequestID(response.Request),
			Time:          time.Now().Format(time.RFC3339Nano),
		}

		logMsg, err := json.Marshal(syncLogMsg)

		if err != nil {
			log.Debugf("Marshall synclog error %s", err)
			return
		}

		multiPartRoundTripper.syncLog.Println(string(logMsg))

		log.Debugf("Sent a multi part upload migration request of object %s to backend %s", response.Request.URL.Path, backendHostName)
	}
}
