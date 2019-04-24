package auth

import (
	"errors"
	"github.com/allegro/akubra/utils"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/allegro/akubra/crdstore"
	"github.com/allegro/akubra/httphandler"
	"github.com/allegro/akubra/log"
	"github.com/wookie41/minio-go/pkg/s3signer"
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

var v4IgnoredHeaders = map[string]bool{
	"Authorization":   true,
	"Content-Type":    true,
	"Content-Length":  true,
	"User-Agent":      true,
	"Connection":      true,
	"X-Forwarded-For": true,
}

var noHeadersIgnored = make(map[string]bool)

// DoesSignMatch - Verify authorization header with calculated header
// returns true if matches, false otherwise. if error is not nil then it is always false
func DoesSignMatch(r *http.Request, cred Keys, ignoredV2CanonicalizedHeaders map[string]bool) APIErrorCode {
	authHeader, err := extractAuthHeader(r.Header)
	if err != ErrNone {
		if err == ErrAuthHeaderEmpty {
			return ErrNone
		}
		return err
	}

	switch authHeader.Version {
	case utils.SignV2Algorithm:
		result, err := s3signer.VerifyV2(r, cred.SecretAccessKey, ignoredV2CanonicalizedHeaders)
		if err != nil {
			reqID := r.Context().Value(log.ContextreqIDKey)
			log.Printf("Error while verifying V2 Signature for request %s: %s", reqID, err)
		}
		if !result {
			return ErrSignatureDoesNotMatch
		}
	case utils.SignV4Algorithm:
		result, err := s3signer.VerifyV4(r, cred.SecretAccessKey)
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

func extractAuthHeader(headers http.Header) (*utils.ParsedAuthorizationHeader, APIErrorCode) {
	gotAuth := headers.Get("Authorization")
	if gotAuth == "" {
		return nil, ErrAuthHeaderEmpty
	}
	authHeader, err := utils.ParseAuthorizationHeader(gotAuth)
	if err != nil {
		return nil, ErrIncorrectAuthHeader
	}
	return &authHeader, ErrNone
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
	rt                            http.RoundTripper
	keys                          Keys
	ignoredV2CanonicalizedHeaders map[string]bool
}

// RoundTrip implements http.RoundTripper interface
func (art authRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if DoesSignMatch(req, art.keys, art.ignoredV2CanonicalizedHeaders) == ErrNone {
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
	rt                            http.RoundTripper
	keys                          Keys
	region                        string
	host                          string
	ignoredV2CanonicalizedHeaders map[string]bool
}

type signAuthServiceRoundTripper struct {
	rt                            http.RoundTripper
	crd                           *crdstore.CredentialsStore
	backend                       string
	host                          string
	ignoredV2CanonicalizedHeaders map[string]bool
}

// RoundTrip implements http.RoundTripper interface
func (srt signRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	authHeader, err := utils.ParseAuthorizationHeader(req.Header.Get("Authorization"))
	if err != nil {
		if err == utils.ErrNoAuthHeader {
			return srt.rt.RoundTrip(req)
		}
		return &http.Response{StatusCode: http.StatusBadRequest, Request: req}, err
	}
	if DoesSignMatch(req, Keys{AccessKeyID: srt.keys.AccessKeyID, SecretAccessKey: srt.keys.SecretAccessKey}, srt.ignoredV2CanonicalizedHeaders) != ErrNone {
		return &http.Response{StatusCode: http.StatusForbidden, Request: req}, err
	}
	req, err = sign(req, authHeader, srt.host, srt.keys.AccessKeyID, srt.keys.SecretAccessKey)
	if err != nil {
		return &http.Response{StatusCode: http.StatusBadRequest, Request: req}, err
	}
	return srt.rt.RoundTrip(req)
}

// RoundTrip implements http.RoundTripper interface
func (srt signAuthServiceRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	authHeader, err := utils.ParseAuthorizationHeader(req.Header.Get("Authorization"))
	if err != nil {
		if err == utils.ErrNoAuthHeader {
			return srt.rt.RoundTrip(req)
		}
		return &http.Response{StatusCode: http.StatusBadRequest, Request: req}, err
	}
	csd, err := srt.crd.Get(authHeader.AccessKey, "akubra")
	if err == crdstore.ErrCredentialsNotFound {
		return &http.Response{StatusCode: http.StatusForbidden, Request: req}, err
	}
	if err != nil {
		return &http.Response{StatusCode: http.StatusInternalServerError, Request: req}, err
	}
	if DoesSignMatch(req, Keys{AccessKeyID: csd.AccessKey, SecretAccessKey: csd.SecretKey}, srt.ignoredV2CanonicalizedHeaders) != ErrNone {
		return &http.Response{StatusCode: http.StatusForbidden, Request: req}, err
	}

	csd, err = srt.crd.Get(authHeader.AccessKey, srt.backend)
	if err == crdstore.ErrCredentialsNotFound {
		return &http.Response{StatusCode: http.StatusForbidden, Request: req}, err
	}
	if err != nil {
		return &http.Response{StatusCode: http.StatusInternalServerError, Request: req}, err
	}
	req, err = sign(req, authHeader, srt.host, csd.AccessKey, csd.SecretKey)
	if err != nil {
		return &http.Response{StatusCode: http.StatusBadRequest, Request: req}, err
	}
	return srt.rt.RoundTrip(req)
}

func isStreamingRequest(req *http.Request) (bool, uint64, error) {
	if req.Header.Get("X-Amz-Content-Sha256") != "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" {
		return false, 0, nil
	}
	if req.Header.Get("Content-Length") == "" {
		return true, 0, errors.New("content-length header missing")
	}
	contentLength, err := strconv.Atoi(req.Header.Get("Content-Length"))
	if err != nil {
		return true, 0, errors.New("failed to parse Content-Lenght header")
	}
	return true, uint64(contentLength), nil
}

// SignDecorator will recompute auth headers for new Key
func SignDecorator(keys Keys, region, host string, ignoredV2CanonicalizedHeaders map[string]bool) httphandler.Decorator {
	return func(rt http.RoundTripper) http.RoundTripper {
		return signRoundTripper{rt: rt, region: region, host: host, keys: keys, ignoredV2CanonicalizedHeaders: ignoredV2CanonicalizedHeaders}
	}
}

// SignAuthServiceDecorator will compute
func SignAuthServiceDecorator(backend, credentialsStoreName, host string, ignoredV2CanonicalizedHeaders map[string]bool) httphandler.Decorator {
	return func(rt http.RoundTripper) http.RoundTripper {
		credentialsStore, err := crdstore.GetInstance(credentialsStoreName)
		if err != nil {
			log.Fatalf("CredentialsStore `%s` is not defined", credentialsStoreName)
		}
		return signAuthServiceRoundTripper{
			rt: rt, backend: backend, host: host, crd: credentialsStore,
			ignoredV2CanonicalizedHeaders: ignoredV2CanonicalizedHeaders}
	}
}

type forceSignRoundTripper struct {
	rt                            http.RoundTripper
	keys                          Keys
	methods                       string
	host                          string
	ignoredV2CanonicalizedHeaders map[string]bool
}

// RoundTrip implements http.RoundTripper interface
func (srt forceSignRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if srt.shouldBeSigned(req) {
		req = s3signer.SignV2(req, srt.keys.AccessKeyID, srt.keys.SecretAccessKey, srt.ignoredV2CanonicalizedHeaders)
	}
	return srt.rt.RoundTrip(req)
}

func sign(req *http.Request, authHeader utils.ParsedAuthorizationHeader, newHost, accessKey, secretKey string) (*http.Request, error) {
	req.Host = newHost
	req.URL.Host = newHost
	switch authHeader.Version {
	case utils.SignV2Algorithm:
		return s3signer.SignV2(req, accessKey, secretKey, noHeadersIgnored), nil
	case utils.SignV4Algorithm:
		isStreamingRequest, dataLen, err := isStreamingRequest(req)
		if isStreamingRequest {
			if err != nil {
				return nil, err
			}
			return s3signer.StreamingSignV4WithIgnoredHeaders(req, accessKey, secretKey, "", authHeader.Region, authHeader.Service, int64(dataLen), time.Now().UTC(), v4IgnoredHeaders), nil
		}
		return s3signer.SignV4WithIgnoredHeaders(req, accessKey, secretKey, "", authHeader.Region, authHeader.Service, v4IgnoredHeaders), nil
	}
	return req, nil
}

func (srt forceSignRoundTripper) shouldBeSigned(request *http.Request) bool {
	if len(srt.methods) == 0 || strings.Contains(srt.methods, request.Method) {
		return true
	}
	return false
}

// ForceSignDecorator will recompute auth headers for new Key
func ForceSignDecorator(keys Keys, host, methods string, ignoredV2CanonicalizedHeaders map[string]bool) httphandler.Decorator {
	return func(rt http.RoundTripper) http.RoundTripper {
		return forceSignRoundTripper{rt: rt, host: host, keys: keys, methods: methods, ignoredV2CanonicalizedHeaders: ignoredV2CanonicalizedHeaders}
	}
}
