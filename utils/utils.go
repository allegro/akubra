package utils

import (
	"bytes"
	"encoding/xml"
	"errors"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/allegro/akubra/log"
	auth2 "github.com/allegro/akubra/storages/auth"
	"github.com/allegro/akubra/types"
)

// BackendError interface helps logging inconsistencies
type BackendError interface {
	Backend() string
	Err() error
	Error() string
}

// RequestID extracts the request id from context
func RequestID(req *http.Request) string {
	if req == nil {
		return ""
	}
	reqIDContextValue := req.Context().Value(log.ContextreqIDKey)
	if reqIDContextValue == nil {
		return ""
	}
	return reqIDContextValue.(string)
}

// ExtractAccessKey extracts s3 auth key from header
func ExtractAccessKey(req *http.Request) string {
	if req.Header == nil {
		log.Debugf("failed to extract access key from req %s - no headers present", req.Context().Value(log.ContextreqIDKey))
		return ""
	}
	authHeader := req.Header.Get("Authorization")
	if authHeader == "" {
		log.Debugf("failed to extract access key from req %s - authorization headers is missing", req.Context().Value(log.ContextreqIDKey))
		return ""
	}
	parsedAuthHeader, parsingErr := auth2.ParseAuthorizationHeader(authHeader)
	if parsingErr != nil {
		log.Debugf("failed to extract access key from req %s - %s, auth header = %s", req.Context().Value(log.ContextreqIDKey), parsingErr, authHeader)
		return ""
	}
	return parsedAuthHeader.AccessKey
}

// ExtractBucketAndKey extract object's bucket and key from request URL
func ExtractBucketAndKey(requestPath string) (string, string) {
	trimmedPath := strings.Trim(requestPath, "/")
	if trimmedPath == "" {
		return "", ""
	}
	pathParts := strings.Split(trimmedPath, "/")
	if len(pathParts) < 2 { 
		return "", ""
	}
	return pathParts[0], pathParts[1]
}
// IsBucketPath check if a given path is a bucket path
func IsBucketPath(path string) bool {
	trimmedPath := strings.Trim(path, "/")
	if trimmedPath == "" {
		return false
	}
	return len(strings.Split(trimmedPath, "/")) == 1
}

//IsMultiPartUploadRequest checks if a request is a multipart upload request
func IsMultiPartUploadRequest(request *http.Request) bool {
	return IsInitiateMultiPartUploadRequest(request) || containsUploadID(request)
}

//IsInitiateMultiPartUploadRequest checks if a request is an initiate multipart upload request
func IsInitiateMultiPartUploadRequest(request *http.Request) bool {
	reqQuery := request.URL.Query()
	_, has := reqQuery["uploads"]
	return has
}

func containsUploadID(request *http.Request) bool {
	reqQuery := request.URL.Query()
	_, has := reqQuery["uploadId"]
	return has
}

//ExtractMultiPartUploadIDFrom extract multipart upload id from http response
func ExtractMultiPartUploadIDFrom(response *http.Response) (string, error) {
	responseBodyBytes, bodyReadError := ioutil.ReadAll(response.Body)
	if bodyReadError != nil {
		return "", bodyReadError
	}
	err := response.Body.Close()
	if err != nil {
		return "", nil
	}
	response.Body = ioutil.NopCloser(bytes.NewBuffer(responseBodyBytes))

	var initiateMultipartUploadResult types.InitiateMultipartUploadResult
	err = xml.Unmarshal(responseBodyBytes, &initiateMultipartUploadResult)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(initiateMultipartUploadResult.UploadID) == "" {
		return "", errors.New("upload ID was empty")
	}
	return initiateMultipartUploadResult.UploadID, nil
}