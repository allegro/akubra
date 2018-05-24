package utils

import (
	"net/http"
	"strings"

	"github.com/allegro/akubra/log"
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
	auth := req.Header.Get("Authorization")
	if auth == "" {
		return ""
	}
	chunks := strings.Split(auth, " ")
	if len(chunks) < 2 || strings.TrimSpace(chunks[0]) != "AWS" {
		return ""
	}
	sigChunk := strings.Split(chunks[1], ":")
	if len(chunks) < 2 {
		return ""
	}
	return strings.TrimSpace(sigChunk[0])
}
