package utils

import (
	"net/http"
	"strings"

	"github.com/allegro/akubra/log"
	auth2 "github.com/allegro/akubra/storages/auth"
)

const (
	// ClusterName is a constant used to put/get cluster's name from request's context
	ClusterName = "Cluster-Name"
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
		return ""
	}
	authHeader := req.Header.Get("Authorization")
	if authHeader == "" {
		return ""
	}
	parsedAuthHeader, parsingErr := auth2.ParseAuthorizationHeader(authHeader)
	if parsingErr != nil {
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
