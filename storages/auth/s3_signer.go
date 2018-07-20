package auth

import (
	"fmt"
	"net/http"
	"strings"

	"regexp"

	"github.com/allegro/akubra/crdstore"
	"github.com/allegro/akubra/httphandler"
	"github.com/allegro/akubra/log"
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
	regexV2Algorithm = "AWS +(?P<access_key>[a-zA-Z0-9_-]+):(?P<Signature>(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?)"
	regexV4Algorithm = "AWS4-HMAC-SHA256 +Credential=(?P<access_key>[a-zA-Z0-9_-]+)/[0-9]+/(?P<Region>[a-zA-Z0-9-]*)/[a-zA-Z0-9-]+/aws4_request,( +)?SignedHeaders=(?P<signed_headers>[a-z0-9-;]+),( +)?Signature=(?P<Signature>[a-z0-9]+)"
)

var reV2 = regexp.MustCompile(regexV2Algorithm)
var reV4 = regexp.MustCompile(regexV4Algorithm)

//ParsedAuthorizationHeader holds the parsed "Authorization" header content
type ParsedAuthorizationHeader struct {
	Version       string
	AccessKey     string
	Signature     string
	SignedHeaders string
	Region        string
}

// DoesSignMatch - Verify authorization header with calculated header
// returns true if matches, false otherwise. if error is not nil then it is always false
func DoesSignMatch(r *http.Request, cred Keys) APIErrorCode {
	gotAuth := r.Header.Get("Authorization")
	if gotAuth == "" {
		return ErrAuthHeaderEmpty
	}
	authHeader, err := ParseAuthorizationHeader(gotAuth)
	if err != nil {
		return ErrIncorrectAuthHeader
	}

	switch authHeader.Version {
	case signV2Algorithm:
		result, err := s3signer.VerifyV2(*r, cred.SecretAccessKey)
		if err != nil {
			reqID := r.Context().Value(log.ContextreqIDKey)
			log.Printf("Error while verifying V2 Signature for request %s: %s", reqID, err)
		}
		if !result {
			return ErrSignatureDoesNotMatch
		}
	case signV4Algorithm:
		result, err := s3signer.VerifyV4(*r, cred.SecretAccessKey)
		if err != nil {
			reqID := r.Context().Value(log.ContextreqIDKey)
			log.Printf("Error while verifying V4 Signature for request %s: %s", reqID, err)
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

// S3Decorator checks if request Signature matches s3 keys
func S3Decorator(keys Keys) httphandler.Decorator {
	return func(rt http.RoundTripper) http.RoundTripper {
		return authRoundTripper{keys: keys}
	}
}

type signRoundTripper struct {
	rt     http.RoundTripper
	keys   Keys
	region string
	host   string
}

type signAuthServiceRoundTripper struct {
	rt      http.RoundTripper
	crd     *crdstore.CredentialsStore
	backend string
	region  string
	host    string
}

// RoundTrip implements http.RoundTripper interface
func (srt signRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	authHeader, err := ParseAuthorizationHeader(req.Header.Get("Authorization"))
	if err != nil {
		return &http.Response{StatusCode: http.StatusBadRequest, Request: req}, err
	}

	req = s3signer.SignV2(*req, srt.keys.AccessKeyID, srt.keys.SecretAccessKey)
	switch authHeader.Version {
	case signV2Algorithm:
		req = s3signer.SignV2(*req, srt.keys.AccessKeyID, srt.keys.SecretAccessKey)
	case signV4Algorithm:
		req.URL.Host = srt.host
		req = s3signer.SignV4(*req, srt.keys.AccessKeyID, srt.keys.SecretAccessKey, "", srt.region)
	}
	return srt.rt.RoundTrip(req)
}

// ParseAuthorizationHeader - extract S3 authorization header details
func ParseAuthorizationHeader(authorizationHeader string) (authHeader ParsedAuthorizationHeader, err error) {
	if reV2.MatchString(authorizationHeader) {
		match := reV2.FindStringSubmatch(authorizationHeader)
		return ParsedAuthorizationHeader{AccessKey: match[1], Signature: match[2], Version: signV2Algorithm}, nil
	}

	if reV4.MatchString(authorizationHeader) {
		match := reV4.FindStringSubmatch(authorizationHeader)
		return ParsedAuthorizationHeader{AccessKey: match[1], Signature: match[6], Region: match[2], SignedHeaders: match[4], Version: signV4Algorithm}, nil
	}

	return ParsedAuthorizationHeader{}, fmt.Errorf("cannot find correct authorization header")
}

// RoundTrip implements http.RoundTripper interface
func (srt signAuthServiceRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	authHeader, err := ParseAuthorizationHeader(req.Header.Get("Authorization"))
	if err != nil {
		return &http.Response{StatusCode: http.StatusBadRequest, Request: req}, err
	}

	csd, err := srt.crd.Get(authHeader.AccessKey, "akubra")
	if err == crdstore.ErrCredentialsNotFound {
		return &http.Response{StatusCode: http.StatusForbidden, Request: req}, err
	}
	if err != nil {
		return &http.Response{StatusCode: http.StatusInternalServerError, Request: req}, err
	}
	if DoesSignMatch(req, Keys{AccessKeyID: csd.AccessKey, SecretAccessKey: csd.SecretKey}) != ErrNone {
		return &http.Response{StatusCode: http.StatusForbidden, Request: req}, err
	}

	csd, err = srt.crd.Get(authHeader.AccessKey, srt.backend)
	if err == crdstore.ErrCredentialsNotFound {
		return &http.Response{StatusCode: http.StatusForbidden, Request: req}, err
	}
	if err != nil {
		return &http.Response{StatusCode: http.StatusInternalServerError, Request: req}, err
	}

	req.Host = srt.host
	req.URL.Host = srt.host
	switch authHeader.Version {
	case signV2Algorithm:
		req = s3signer.SignV2(*req, csd.AccessKey, csd.SecretKey)
	case signV4Algorithm:
		req = s3signer.SignV4(*req, csd.AccessKey, csd.SecretKey, "", srt.region)
	}
	return srt.rt.RoundTrip(req)
}

// SignDecorator will recompute auth headers for new Key
func SignDecorator(keys Keys, region, host string) httphandler.Decorator {
	return func(rt http.RoundTripper) http.RoundTripper {
		return signRoundTripper{rt: rt, region: region, host: host, keys: keys}
	}
}

// SignAuthServiceDecorator will compute
func SignAuthServiceDecorator(backend, region, endpoint, host string) httphandler.Decorator {
	return func(rt http.RoundTripper) http.RoundTripper {
		credentialsStore, err := crdstore.GetInstance(endpoint)
		if err != nil {
			log.Fatalf("error CredentialsStore `%s` is not defined", endpoint)
		}
		return signAuthServiceRoundTripper{rt: rt, backend: backend, region: region, host: host, crd: credentialsStore}
	}
}

type forceSignRoundTripper struct {
	rt      http.RoundTripper
	keys    Keys
	methods string
	region  string
	host    string
}

// RoundTrip implements http.RoundTripper interface
func (srt forceSignRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if srt.shouldBeSigned(req) {
		req = s3signer.SignV2(*req, srt.keys.AccessKeyID, srt.keys.SecretAccessKey)
	}
	return srt.rt.RoundTrip(req)
}
func (srt forceSignRoundTripper) shouldBeSigned(request *http.Request) bool {
	if len(srt.methods) == 0 || strings.Contains(srt.methods, request.Method) {
		return true
	}

	return false
}

// ForceSignDecorator will recompute auth headers for new Key
func ForceSignDecorator(keys Keys, region, host, methods string) httphandler.Decorator {
	return func(rt http.RoundTripper) http.RoundTripper {
		return forceSignRoundTripper{rt: rt, region: region, host: host, keys: keys, methods: methods}
	}
}
