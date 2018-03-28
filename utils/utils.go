package utils

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/allegro/akubra/log"
	"github.com/allegro/akubra/transport"
)

const (
	// InternalBucketHeader used for rewriting domain style
	InternalBucketHeader = "X-Akubra-Internal-Bucket-lejK0EpVZy"
	// InternalPathStyleFlag indicates if an request is path style
	InternalPathStyleFlag = "X-Akubra-Internal-Path-Style-lSd29csa"
	// EmptyString empty string
	EmptyString = ""
)

const (
	hostRewritten = "rewritten host for request %d to %s"
	missingBucket = "missing bucket header, can't rewrite to path style, request id %d"
	styleRewritten = "rewritten domain style url (%s) to path style url (%s) for request %s"
	pathStyleFormat = "/%s%s"
)

// BackendError interface helps logging inconsistencies
type BackendError interface {
	Backend() string
	Err() error
	Error() string
}

// RequestID extracts the request id from context
func RequestID(req *http.Request) string {
	return req.Context().Value(log.ContextreqIDKey).(string)
}

// ExtractDestinationHostName extract destination hostname fromrequest
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

// ExtractAccessKey extracts s3 auth key from header
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
	return request.Header.Get(InternalBucketHeader) != EmptyString
}

// RewriteHostAndBucket rewrites url if needed
func RewriteHostAndBucket(req *http.Request, backendEndpoint *url.URL, forcePathStyle bool) error {
	bucket := req.Header.Get(InternalBucketHeader)
	requestIsNotPathStyle := req.Header.Get(InternalPathStyleFlag) == EmptyString

	if forcePathStyle && requestIsNotPathStyle {
		if bucket == EmptyString {
			return fmt.Errorf(missingBucket, req.Context().Value(log.ContextreqIDKey))
		}
		pathStyleURL := fmt.Sprintf(pathStyleFormat, bucket, req.URL.Path)
		log.Debugf(styleRewritten, req.URL.Path, pathStyleURL, req.Context().Value(log.ContextreqIDKey))
		req.URL.Path = pathStyleURL
	}

	req.URL.Scheme = backendEndpoint.Scheme
	if IsDomainStyleRequest(req) && !forcePathStyle {
		if bucket == EmptyString {
			return fmt.Errorf(missingBucket, req.Context().Value(log.ContextreqIDKey))
		}
		hostWithBucketName := fmt.Sprintf("%s.%s", bucket, backendEndpoint.Host)
		req.Host = hostWithBucketName
		req.URL.Host = hostWithBucketName
	} else {
		req.Host = backendEndpoint.Host
		req.URL.Host = backendEndpoint.Host
	}

	log.Debugf(hostRewritten, req.Context().Value(log.ContextreqIDKey), req.Host)
	return nil
}