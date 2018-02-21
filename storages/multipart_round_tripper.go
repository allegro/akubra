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

// MultiPartRoundTripper handles the multipart upload. If multipart upload is detected, it delegates the request
// to handle the operation in standard fashion
// to the backend selected using the active backends hash ring, otherwise the cluster round tripper is used
type MultiPartRoundTripper struct {
	fallBackRoundTripper  http.RoundTripper
	syncLog               log.Logger
	backendsRoundTrippers map[string]*Backend
	backendsRing          *hashring.HashRing
	backendsEndpoints     []string
}

// NewMultiPartRoundTripper constructs a new MultiPartRoundTripper and returns a pointer to it
func NewMultiPartRoundTripper(cluster *Cluster, syncLog log.Logger) *MultiPartRoundTripper {

	multiPartRoundTripper := &MultiPartRoundTripper{
		fallBackRoundTripper: cluster.transport,
		syncLog:              syncLog,
	}

	multiPartRoundTripper.setupRoundTripper(cluster.Backends())
	return multiPartRoundTripper
}

func (multiPartRoundTripper *MultiPartRoundTripper) setupRoundTripper(backends []http.RoundTripper) {

	var backendsEndpoints []string
	var activeBackendsEndpoints []string

	multiPartRoundTripper.backendsRoundTrippers = make(map[string]*Backend)

	for _, roundTripper := range backends {

		if backend, isBackendType := roundTripper.(*Backend); isBackendType {

			if !backend.Maintenance {
				multiPartRoundTripper.backendsRoundTrippers[backend.Endpoint.String()] = backend
				activeBackendsEndpoints = append(activeBackendsEndpoints, backend.Endpoint.String())
			}

			backendsEndpoints = append(backendsEndpoints, backend.Endpoint.String())
		}
	}

	multiPartRoundTripper.backendsEndpoints = backendsEndpoints
	multiPartRoundTripper.backendsRing = hashring.New(activeBackendsEndpoints)
}

// RoundTrip performs a RoundTrip using the strategy described in MultiPartRoundTripper
func (multiPartRoundTripper *MultiPartRoundTripper) RoundTrip(request *http.Request) (response *http.Response, requestError error) {

	if isMultiPartUploadRequest(request) {

		if !multiPartRoundTripper.canHandleMultiUpload() {
			log.Debugf("Multi upload for %s failed - no backends available.", request.URL.Path)
			return nil, errors.New("Can't handle multi upload")
		}

		multiUploadBackend, backendSelectError := multiPartRoundTripper.pickBackend(request.URL.Path)

		if backendSelectError != nil {
			log.Debugf("Multi upload failed for %s - %s", backendSelectError, request.URL.Path)
			return nil, errors.New("Can't handle multi upload")
		}

		log.Debugf("Handling multipart upload, sending %s to %s, RequestID id %s",
			request.URL.Path,
			multiUploadBackend.Endpoint,
			request.Context().Value(log.ContextreqIDKey))

		response, requestError = multiUploadBackend.RoundTrip(request)

		if requestError != nil {
			log.Debugf("Error during multipart upload: %s", requestError)
			return
		}

		if !isInitiateRequest(request) && isCompleteUploadResponseSuccessful(response) {
			go multiPartRoundTripper.reportCompletionToMigrator(response)
		}

		log.Debugf("Served multipart request, response code %d, status %s", response.StatusCode, response.Status)

		return
	}

	return multiPartRoundTripper.fallBackRoundTripper.RoundTrip(request)
}

func (multiPartRoundTripper *MultiPartRoundTripper) pickBackend(objectPath string) (*Backend, error) {

	backendEndpoint, nodeFound := multiPartRoundTripper.backendsRing.GetNode(objectPath)

	if !nodeFound {
		return nil, errors.New("Can't find backned for upload in multi uplaod ring")
	}

	backend, backendFound := multiPartRoundTripper.backendsRoundTrippers[backendEndpoint]

	if !backendFound {
		return nil, errors.New("Can't find backend for upload in backendsRoundTripper")
	}

	return backend, nil
}

func (multiPartRoundTripper *MultiPartRoundTripper) canHandleMultiUpload() bool {
	return len(multiPartRoundTripper.backendsRoundTrippers) > 0
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

	for _, destBackendEndpoint := range multiPartRoundTripper.backendsEndpoints {

		if destBackendEndpoint == response.Request.Host {
			continue
		}

		syncLogMsg := &httphandler.SyncLogMessageData{
			Method:        "PUT",
			FailedHost:    destBackendEndpoint,
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

		log.Debugf("Sent a multipart upload migration request of object %s to backend %s", response.Request.URL.Path, destBackendEndpoint)
	}
}
