package utils

import (
	"net/http"

	"github.com/allegro/akubra/log"
	auth2 "github.com/allegro/akubra/storages/auth"
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
