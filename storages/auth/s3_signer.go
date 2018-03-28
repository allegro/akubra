package auth

import (
	"fmt"
	"net/http"
	"net/url"

	"regexp"

	"github.com/allegro/akubra/crdstore"
	"github.com/allegro/akubra/httphandler"
	"github.com/allegro/akubra/log"
	"github.com/allegro/akubra/utils"
	"github.com/bnogas/minio-go/pkg/s3signer"
)

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
	signV2Algorithm  = "AWS"
	signV4Algorithm  = "AWS4-HMAC-SHA256"
	regexV2Algorithm = "AWS +(?P<access_key>[a-zA-Z0-9_-]+):(?P<signature>(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?)"
	regexV4Algorithm = "AWS4-HMAC-SHA256 +Credential=(?P<access_key>[a-zA-Z0-9_-]+)/[0-9]+/(?P<region>[a-zA-Z0-9-]*)/[a-zA-Z0-9-]+/aws4_request,( +)?SignedHeaders=(?P<signed_headers>[a-z0-9-;]+),( +)?Signature=(?P<signature>[a-z0-9]+)"
)

var reV2 = regexp.MustCompile(regexV2Algorithm)
var reV4 = regexp.MustCompile(regexV4Algorithm)

type parsedAuthorizationHeader struct {
	version       string
	accessKey     string
	signature     string
	signedHeaders string
	region        string
}

// DoesSignMatch - Verify authorization header with calculated header
// returns true if matches, false otherwise. if error is not nil then it is always false
func DoesSignMatch(r *http.Request, cred Keys) APIErrorCode {
	gotAuth := r.Header.Get("Authorization")
	if gotAuth == "" {
		return ErrAuthHeaderEmpty
	}
	authHeader, err := extractAuthorizationHeader(gotAuth)
	if err != nil {
		return ErrIncorrectAuthHeader
	}

	switch authHeader.version {
	case signV2Algorithm:
		result, err := s3signer.VerifyV2(*r, cred.SecretAccessKey)
		if err != nil {
			reqID := r.Context().Value(log.ContextreqIDKey)
			log.Printf("Error while verifying V2 signature for request %s: %s", reqID, err)
		}
		if !result {
			return ErrSignatureDoesNotMatch
		}
	case signV4Algorithm:
		result, err := s3signer.VerifyV4(*r, cred.SecretAccessKey)
		if err != nil {
			reqID := r.Context().Value(log.ContextreqIDKey)
			log.Printf("Error while verifying V4 signature for request %s: %s", reqID, err)
		}
		if !result {
			return ErrSignatureDoesNotMatch
		}
	default:
		return ErrUnsupportedSignatureVersion
	}

	return ErrNone
}

// Keys user credentials
type Keys struct {
	AccessKeyID     string `json:"access-key" yaml:"AccessKey"`
	SecretAccessKey string `json:"secret-key" yaml:"Secret"`
}

func responseForbidden(req *http.Request) *http.Response {
	return &http.Response{
		Status:     "403 Forbidden",
		StatusCode: http.StatusForbidden,
		Proto:      req.Proto,
		ProtoMajor: req.ProtoMajor,
		ProtoMinor: req.ProtoMinor,
		Request:    req,
	}
}

type authRoundTripper struct {
	rt   http.RoundTripper
	keys Keys
}

// RoundTrip implements http.RoundTripper interface
func (art authRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if DoesSignMatch(req, art.keys) == ErrNone {
		return art.rt.RoundTrip(req)
	}
	return responseForbidden(req), nil
}

// S3Decorator checks if request signature matches s3 keys
func S3Decorator(keys Keys) httphandler.Decorator {
	return func(rt http.RoundTripper) http.RoundTripper {
		return authRoundTripper{keys: keys}
	}
}

type requestFormatRoundTripper struct {
	rt             	http.RoundTripper
	forcePathStyle 	bool
	backendEndpoint *url.URL
}

type signRoundTripper struct {
	rt             http.RoundTripper
	keys           Keys
	region         string
	host           *url.URL
	forcePathStyle bool
}

type signAuthServiceRoundTripper struct {
	rt             http.RoundTripper
	crd            *crdstore.CredentialsStore
	backend        string
	region         string
	host           *url.URL
	forcePathStyle bool
}

// RoundTrip implements http.RoundTripper interface
func (passthroughRoundTripper requestFormatRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	err := utils.RewriteHostAndBucket(req, passthroughRoundTripper.backendEndpoint, passthroughRoundTripper.forcePathStyle)
	if err != nil {
		return nil, err
	}
	return passthroughRoundTripper.rt.RoundTrip(req)
}

// RoundTrip implements http.RoundTripper interface
func (srt signRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	authHeader, err := extractAuthorizationHeader(req.Header.Get("Authorization"))
	if err != nil {
		return &http.Response{StatusCode: http.StatusBadRequest, Request: req}, err
	}
	if DoesSignMatch(req, srt.keys) != ErrNone {
		return &http.Response{StatusCode: http.StatusForbidden, Request: req}, err
	}

	rewritingError := utils.RewriteHostAndBucket(req, srt.host, srt.forcePathStyle)
	if rewritingError != nil {
		return nil, err
	}

	switch authHeader.version {
	case signV2Algorithm:
		req = s3signer.SignV2(*req, srt.keys.AccessKeyID, srt.keys.SecretAccessKey)
	case signV4Algorithm:
		req = s3signer.SignV4(*req, srt.keys.AccessKeyID, srt.keys.SecretAccessKey, "", srt.region)
	}
	return srt.rt.RoundTrip(req)
}

// extractAuthorizationHeader - extract S3 authorization header details
func extractAuthorizationHeader(authorizationHeader string) (authHeader parsedAuthorizationHeader, err error) {
	if reV2.MatchString(authorizationHeader) {
		match := reV2.FindStringSubmatch(authorizationHeader)
		return parsedAuthorizationHeader{accessKey: match[1], signature: match[2], version: signV2Algorithm}, nil
	}

	if reV4.MatchString(authorizationHeader) {
		match := reV4.FindStringSubmatch(authorizationHeader)
		return parsedAuthorizationHeader{accessKey: match[1], signature: match[6], region: match[2], signedHeaders: match[4], version: signV4Algorithm}, nil
	}

	return parsedAuthorizationHeader{}, fmt.Errorf("cannot find correct authorization header")
}

// RoundTrip implements http.RoundTripper interface
func (srt signAuthServiceRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	authHeader, err := extractAuthorizationHeader(req.Header.Get("Authorization"))
	if err != nil {
		return &http.Response{StatusCode: http.StatusBadRequest, Request: req}, err
	}

	csd, err := srt.crd.Get(authHeader.accessKey, "akubra")
	if err == crdstore.ErrCredentialsNotFound {
		return &http.Response{StatusCode: http.StatusForbidden, Request: req}, err
	}
	if err != nil {
		return &http.Response{StatusCode: http.StatusInternalServerError, Request: req}, err
	}

	if DoesSignMatch(req, Keys{AccessKeyID: csd.AccessKey, SecretAccessKey: csd.SecretKey}) != ErrNone {
		return &http.Response{StatusCode: http.StatusForbidden, Request: req}, err
	}

	csd, err = srt.crd.Get(authHeader.accessKey, srt.backend)
	if err == crdstore.ErrCredentialsNotFound {
		return &http.Response{StatusCode: http.StatusForbidden, Request: req}, err
	}
	if err != nil {
		return &http.Response{StatusCode: http.StatusInternalServerError, Request: req}, err
	}

	rewritingError := utils.RewriteHostAndBucket(req, srt.host, srt.forcePathStyle)
	if rewritingError != nil {
		return nil, err
	}

	switch authHeader.version {
	case signV2Algorithm:
		req = s3signer.SignV2(*req, csd.AccessKey, csd.SecretKey)
	case signV4Algorithm:
		req = s3signer.SignV4(*req, csd.AccessKey, csd.SecretKey, "", srt.region)
	}
	return srt.rt.RoundTrip(req)
}

// RequestFormatDecorator rewrites url if needed
func RequestFormatDecorator(backendEndpoint *url.URL, forcePathStyle bool) httphandler.Decorator {
	return func(rt http.RoundTripper) http.RoundTripper {
		return requestFormatRoundTripper{rt :rt, backendEndpoint: backendEndpoint, forcePathStyle: forcePathStyle}
	}
}

// SignDecorator will recompute auth headers for new Key
func SignDecorator(keys Keys, region string, host *url.URL, forcePathStyle bool) httphandler.Decorator {
	return func(rt http.RoundTripper) http.RoundTripper {
		return signRoundTripper{rt: rt, region: region, host: host, keys: keys, forcePathStyle: forcePathStyle}
	}
}

// SignAuthServiceDecorator will compute
func SignAuthServiceDecorator(backend, region, endpoint string, host *url.URL, forcePathStyle bool) httphandler.Decorator {
	return func(rt http.RoundTripper) http.RoundTripper {
		credentialsStore, err := crdstore.GetInstance(endpoint)
		if err != nil {
			log.Fatalf("error CredentialsStore `%s` is not defined", endpoint)
		}
		return signAuthServiceRoundTripper{rt: rt, backend: backend, region: region, host: host,
											crd: credentialsStore, forcePathStyle: forcePathStyle}
	}
}