package utils

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"github.com/allegro/akubra/internal/akubra/log"
	"github.com/allegro/akubra/internal/akubra/types"
)

const (
	//SignV2Algorithm is a prefix for v2 auth header
	SignV2Algorithm = "AWS"
	//SignV4Algorithm indicates a v4
	SignV4Algorithm = "AWS4-HMAC-SHA256"
	//RegexV2Algorithm is a regexp for parsing v2 auth headers
	RegexV2Algorithm = "AWS +(?P<access_key>[a-zA-Z0-9_-]+):(?P<Signature>(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?)"
	//RegexV4Algorithm is a regexp for parsing v4 auth headers
	RegexV4Algorithm = "AWS4-HMAC-SHA256 +Credential=(?P<access_key>.+)/[0-9]+/(?P<region>[a-zA-Z0-9-]*)/(?P<service>[a-zA-Z0-9_-]+)/aws4_request,( +)?SignedHeaders=(?P<signed_headers>[a-z0-9-;.]+),( +)?Signature=(?P<signature>[a-z0-9]+)"
)

//ErrNoAuthHeader indicates that no authorization header was found in the request
var ErrNoAuthHeader = fmt.Errorf("cannot find correct authorization header")

var reV2 = regexp.MustCompile(RegexV2Algorithm)
var reV4 = regexp.MustCompile(RegexV4Algorithm)

//ParsedAuthorizationHeader holds the parsed "Authorization" header content
type ParsedAuthorizationHeader struct {
	Version       string
	AccessKey     string
	Signature     string
	SignedHeaders string
	Region        string
	Service       string
}

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

// ParseAuthorizationHeader - extract S3 authorization header details
func ParseAuthorizationHeader(authorizationHeader string) (authHeader ParsedAuthorizationHeader, err error) {
	if reV2.MatchString(authorizationHeader) {
		match := reV2.FindStringSubmatch(authorizationHeader)
		return ParsedAuthorizationHeader{AccessKey: match[1], Signature: match[2], Version: SignV2Algorithm}, nil
	}
	if reV4.MatchString(authorizationHeader) {
		match := reV4.FindStringSubmatch(authorizationHeader)
		return ParsedAuthorizationHeader{AccessKey: match[1], Signature: match[7], Region: match[2], SignedHeaders: match[5],
			Version: SignV4Algorithm, Service: match[3]}, nil
	}
	return ParsedAuthorizationHeader{}, ErrNoAuthHeader
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
	parsedAuthHeader, parsingErr := ParseAuthorizationHeader(authHeader)
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
	pathParts := strings.SplitN(trimmedPath, "/", 2)
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

//IsObjectPath check if a given path is an object path
func IsObjectPath(path string) bool {
	//TODO add support for domain style paths when the domain support is going to be merged
	parts := strings.SplitN(strings.Trim(path, "/"), "/", 2)
	return len(parts) == 2 && parts[1] != ""
}

//TODO Query() parses the query string every time it's called... this can (and should!) be optimized
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

//ReplicateRequest makes a copy of the provided request
func ReplicateRequest(request *http.Request) (*http.Request, error) {
	replicatedRequest := new(http.Request)
	*replicatedRequest = *request
	replicatedRequest.URL = &url.URL{}
	*replicatedRequest.URL = *request.URL
	replicatedRequest.Header = http.Header{}

	if request.Body != nil {
		bodyReader, err := request.GetBody()
		if err != nil {
			return nil, err
		}
		replicatedRequest.Body = bodyReader
		replicatedRequest.GetBody = func() (io.ReadCloser, error) {
			return request.GetBody()
		}
	}
	replicatedRequest.Header = http.Header{}
	for headerName, headerValues := range request.Header {
		for idx := range headerValues {
			replicatedRequest.Header.Add(headerName, headerValues[idx])
		}
	}
	return replicatedRequest.WithContext(request.Context()), nil
}

//ReadRequestBody returns the bytes of the request body or nil of body is not present
func ReadRequestBody(request *http.Request) ([]byte, error) {
	if request.Body == nil {
		return nil, nil
	}
	bodyBytes, err := ioutil.ReadAll(request.Body)
	if err != nil {
		_ = request.Body.Close()
		return nil, err
	}
	err = request.Body.Close()
	if err != nil {
		return nil, err
	}
	return bodyBytes, nil
}

//PutResponseHeaderToContext extracts the header value from the response and puts it into the context under the specified key
func PutResponseHeaderToContext(context context.Context, contextValueName log.ContextKey, resp *http.Response, headerName string) {
	ctxValue := context.Value(contextValueName).(*string)
	if ctxValue == nil {
		return
	}
	headerValue := resp.Header.Get(headerName)
	*ctxValue = headerValue
}
