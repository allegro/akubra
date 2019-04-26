package utils

import (
	"fmt"
	"github.com/allegro/akubra/log"
	"net/http"
	"regexp"
)

// BackendError interface helps logging inconsistencies
type BackendError interface {
	Backend() string
	Err() error
	Error() string
}

// APIErrorCode type of error status.
type APIErrorCode int

// Error codes, non exhaustive list - http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
const (
	ErrAuthHeaderEmpty APIErrorCode = iota
	ErrSignatureDoesNotMatch
	ErrIncorrectAuthHeader
	ErrUnsupportedSignatureVersion
	ErrNone
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

var reV2 = regexp.MustCompile(RegexV2Algorithm)
var reV4 = regexp.MustCompile(RegexV4Algorithm)

//ParsedAuthorizationHeader holds the parsed "Authorization" header content
type ParsedAuthorizationHeader struct {
	Version       string
	AccessKey     string
	Signature     string
	SignedHeaders string
	Region        string
}

// ParseAuthorizationHeader - extract S3 authorization header details
func ParseAuthorizationHeader(authorizationHeader string) (authHeader ParsedAuthorizationHeader, err error) {
	if reV2.MatchString(authorizationHeader) {
		match := reV2.FindStringSubmatch(authorizationHeader)
		return ParsedAuthorizationHeader{AccessKey: match[1], Signature: match[2], Version: SignV2Algorithm}, nil
	}

	if reV4.MatchString(authorizationHeader) {
		match := reV4.FindStringSubmatch(authorizationHeader)
		return ParsedAuthorizationHeader{AccessKey: match[1], Signature: match[6], Region: match[2], SignedHeaders: match[4], Version: SignV4Algorithm}, nil
	}

	return ParsedAuthorizationHeader{}, fmt.Errorf("cannot find correct authorization header")
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
		return ""
	}
	authHeader := req.Header.Get("Authorization")
	if authHeader == "" {
		return ""
	}
	parsedAuthHeader, parsingErr := ParseAuthorizationHeader(authHeader)
	if parsingErr != nil {
		return ""
	}
	return parsedAuthHeader.AccessKey
}
