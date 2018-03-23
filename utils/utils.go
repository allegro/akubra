package utils

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/allegro/akubra/log"
	"github.com/allegro/akubra/transport"
)

const (
	// InternalHostHeader is used for rewriting domain style
	InternalHostHeader   = "X-Akubra-Internal-Host-3yeLjyjQNx"
	// InternalBucketHeader used for rewriting domain style
	InternalBucketHeader = "X-Akubra-Internal-Bucket-lejK0EpVZy"
	// PathStyleFormat is a S3 path style format
)

// BackendError interface helps logging inconsistencies
type BackendError interface {
	Backend() string
	Err() error
	Error() string
}

//RequestID extracts the request id from context
func RequestID(req *http.Request) string {
	return req.Context().Value(log.ContextreqIDKey).(string)
}

//ExtractDestinationHostName extract destination hostname fromrequest
func ExtractDestinationHostName(r transport.ResErrTuple) string {
	if r.Res != nil {
		return r.Res.Request.URL.Host
	}
	berr, ok := r.Err.(BackendError)
	if ok {
		return berr.Backend()
	}
	log.Printf("Requested backend is not retrievable from tuple %#v", r)
	return ""
}

//ExtractAccessKey extracts s3 auth key from header
func ExtractAccessKey(req *http.Request) string {
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

// IsBucketPath tests if a request is a bucket operation request
func IsBucketPath(request *http.Request) bool {
	path := request.URL.Path
	if IsDomainStyleRequest(request) {
		path = fmt.Sprintf("/%s%s", request.Header.Get(InternalBucketHeader), path)
	}
	trimmedPath := strings.Trim(path, "/")
	if trimmedPath == "" {
		return false
	}
	return len(strings.Split(trimmedPath, "/")) == 1
}

// IsDomainStyleRequest tests if request has a domain style url
func IsDomainStyleRequest(request *http.Request) bool {
	return request.Header.Get(InternalHostHeader) != "" &&
		request.Header.Get(InternalBucketHeader) != ""
}