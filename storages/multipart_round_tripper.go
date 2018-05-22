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
	"github.com/allegro/akubra/storages/backend"
	"github.com/allegro/akubra/types"
	"github.com/allegro/akubra/utils"
	"github.com/serialx/hashring"
)

// MultiPartRoundTripper handles the multipart upload. If multipart upload is detected, it delegates the request
// to the backend selected using the active backends hash ring, otherwise the cluster round tripper is used
// to handle the operation in standard fashion
type MultiPartRoundTripper struct {
	syncLog               log.Logger
	backendsRoundTrippers map[string]*backend.Backend
	backendsRing          *hashring.HashRing
	backendsEndpoints     []string
}

// Cancel Client interface
func (multiPartRoundTripper MultiPartRoundTripper) Cancel() error { return nil }

// newMultiPartRoundTripper initializes multipart client
func newMultiPartRoundTripper(backends []*Backend) client {
	multiPartRoundTripper := &MultiPartRoundTripper{}
	var backendsEndpoints []string
	var activeBackendsEndpoints []string

	multiPartRoundTripper.backendsRoundTrippers = make(map[string]*Backend)

	for _, backend := range backends {
		if !backend.Maintenance {
			multiPartRoundTripper.backendsRoundTrippers[backend.Endpoint.Host] = backend
			activeBackendsEndpoints = append(activeBackendsEndpoints, backend.Endpoint.Host)
		}

		backendsEndpoints = append(backendsEndpoints, backend.Endpoint.Host)
	}

	multiPartRoundTripper.backendsEndpoints = backendsEndpoints
	multiPartRoundTripper.backendsRing = hashring.New(activeBackendsEndpoints)
	return multiPartRoundTripper
}

// Do performs backend request
func (multiPartRoundTripper *MultiPartRoundTripper) Do(request *http.Request) <-chan BackendResponse {
	out := make(chan BackendResponse)
	if !multiPartRoundTripper.canHandleMultiUpload() {
		log.Debugf("Multi upload for %s failed - no backends available.", request.URL.Path)
		go func() {
			out <- BackendResponse{Response: nil, Error: errors.New("Can't handle multi upload")}
			close(out)
		}()
		return out
	}

	multiUploadBackend, backendSelectError := multiPartRoundTripper.pickBackend(request.URL.Path)

	if backendSelectError != nil {
		log.Debugf("Multi upload failed for %s - %s", backendSelectError, request.URL.Path)
		go func() {
			out <- BackendResponse{Response: nil, Error: errors.New("Can't handle multi upload")}
			close(out)
		}()
		return out
	}

	log.Debugf("Handling multipart upload, sending %s to %s, RequestID id %s",
		request.URL.Path,
		multiUploadBackend.Endpoint,
		request.Context().Value(log.ContextreqIDKey))

	httpresponse, requestError := multiUploadBackend.RoundTrip(request)

	if requestError != nil {
		log.Debugf("Error during multipart upload: %s", requestError)

	}
	go func() {
		out <- BackendResponse{Response: httpresponse, Error: requestError, Backend: multiUploadBackend}
		log.Printf("!isInitiateRequest(request) == %t && isCompleteUploadResponseSuccessful(httpresponse)== %t\n", !isInitiateRequest(request), isCompleteUploadResponseSuccessful(httpresponse))
		if !isInitiateRequest(request) && isCompleteUploadResponseSuccessful(httpresponse) {
			for _, backend := range multiPartRoundTripper.backendsRoundTrippers {
				log.Printf("got some backend to report %s", backend.Name)
				if backend != multiUploadBackend {
					out <- BackendResponse{Response: nil, Error: errors.New("Can't handle multi upload"), Backend: backend}
				}
			}
		}
		close(out)
	}()

	return out
}

func (multiPartRoundTripper *MultiPartRoundTripper) pickBackend(objectPath string) (*backend.Backend, error) {

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
	err := response.Body.Close()
	if err != nil {
		log.Println("Could not close response.Body")
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

		if destBackendEndpoint == response.Request.URL.Host {
			continue
		}

		syncLogMsg := &httphandler.SyncLogMessageData{
			Method:        "PUT",
			FailedHost:    destBackendEndpoint,
			SuccessHost:   response.Request.URL.Host,
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
