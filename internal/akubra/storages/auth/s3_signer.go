package auth

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/allegro/akubra/internal/akubra/utils"

	"github.com/allegro/akubra/external/miniotweak/s3signer"
	"github.com/allegro/akubra/internal/akubra/crdstore"
	"github.com/allegro/akubra/internal/akubra/httphandler"
	"github.com/allegro/akubra/internal/akubra/log"
)

// APIErrorCode type of error status.
type APIErrorCode int

// Error codes, non exhaustive list - http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
const (
	ErrSignatureDoesNotMatch APIErrorCode = iota
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

//DoesSignMatch - Verify authorization header with calculated header
//returns true if matches, false otherwise. if error is not nil then it is always false
func DoesSignMatch(r *http.Request, cred Keys, ignoredCanonicalizedHeaders map[string]bool) APIErrorCode {
	authHeaderVal := r.Context().Value(httphandler.AuthHeader)
	if authHeaderVal == nil {
		return ErrNone
	}
	authHeader := authHeaderVal.(*utils.ParsedAuthorizationHeader)

	switch authHeader.Version {
	case utils.SignV2Algorithm:
		result, err := s3signer.VerifyV2(r, cred.SecretAccessKey, ignoredCanonicalizedHeaders)
		if err != nil {
			reqID := utils.RequestID(r)
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

// Keys user credentials
type Keys struct {
	AccessKeyID     string `json:"access-key" yaml:"AccessKey"`
	SecretAccessKey string `json:"secret-key" yaml:"Secret"`
}

type signRoundTripper struct {
	rt                          http.RoundTripper
	keys                        Keys
	region                      string
	host                        string
	ignoredCanonicalizedHeaders map[string]bool
	v4IgnoredHeaders            map[string]bool
}

type signAuthServiceRoundTripper struct {
	rt                          http.RoundTripper
	crd                         *crdstore.CredentialsStore
	backend                     string
	host                        string
	ignoredCanonicalizedHeaders map[string]bool
	v4IgnoredHeaders            map[string]bool
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

	if DoesSignMatch(req, Keys{AccessKeyID: srt.keys.AccessKeyID, SecretAccessKey: srt.keys.SecretAccessKey}, srt.ignoredCanonicalizedHeaders) != ErrNone {
		return &http.Response{StatusCode: http.StatusForbidden, Request: req}, err
	}

	req, err = sign(req, authHeader, srt.host, srt.keys.AccessKeyID, srt.keys.SecretAccessKey, srt.ignoredCanonicalizedHeaders, srt.v4IgnoredHeaders)
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
	csd, err = srt.crd.Get(authHeader.AccessKey, srt.backend)
	if err == crdstore.ErrCredentialsNotFound {
		return &http.Response{StatusCode: http.StatusForbidden, Request: req}, err
	}
	if err != nil {
		return &http.Response{StatusCode: http.StatusInternalServerError, Request: req}, err
	}
	req, err = sign(req, authHeader, srt.host, csd.AccessKey, csd.SecretKey, srt.ignoredCanonicalizedHeaders, srt.v4IgnoredHeaders)
	if err != nil {
		return &http.Response{StatusCode: http.StatusBadRequest, Request: req}, err
	}
	if req == nil {
		return &http.Response{StatusCode: http.StatusInternalServerError, Request: req}, err
	}
	return srt.rt.RoundTrip(req)
}

func isStreamingRequest(req *http.Request) (bool, int64, error) {
	if req.Header.Get("X-Amz-Content-Sha256") != "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" {
		return false, 0, nil
	}
	if req.Header.Get("x-amz-decoded-content-length") == "" {
		return true, 0, errors.New("content-length header missing")
	}
	contentLength, err := strconv.Atoi(req.Header.Get("x-amz-decoded-content-length"))
	if err != nil {
		return true, 0, errors.New("failed to parse x-amz-decoded-content-length header")
	}
	return true, int64(contentLength), nil
}

// SignDecorator will recompute auth headers for new Key
func SignDecorator(keys Keys, region, host string, ignoredCanonicalizedHeaders map[string]bool) httphandler.Decorator {
	return func(rt http.RoundTripper) http.RoundTripper {
		allV4IgnoredHeaders := makeV4IgnoredHeaders(ignoredCanonicalizedHeaders)

		return signRoundTripper{rt: rt,
			region:                      region,
			host:                        host,
			keys:                        keys,
			ignoredCanonicalizedHeaders: ignoredCanonicalizedHeaders,
			v4IgnoredHeaders:            allV4IgnoredHeaders}
	}
}

// SignAuthServiceDecorator will compute
func SignAuthServiceDecorator(backend, credentialsStoreName, host string, ignoredCanonicalizedHeaders map[string]bool) httphandler.Decorator {
	return func(rt http.RoundTripper) http.RoundTripper {
		credentialsStore, err := crdstore.GetInstance(credentialsStoreName)
		if err != nil {
			log.Fatalf("CredentialsStore `%s` is not defined", credentialsStoreName)
		}
		allV4IgnoredHeaders := makeV4IgnoredHeaders(ignoredCanonicalizedHeaders)
		return signAuthServiceRoundTripper{
			rt: rt, backend: backend, host: host, crd: credentialsStore,
			ignoredCanonicalizedHeaders: ignoredCanonicalizedHeaders,
			v4IgnoredHeaders:            allV4IgnoredHeaders}
	}
}

func makeV4IgnoredHeaders(ignoredCanonicalizedHeaders map[string]bool) map[string]bool {
	ignoredHeaders := make(map[string]bool)
	for k, v := range v4IgnoredHeaders {
		ignoredHeaders[k] = v
	}
	for k, v := range ignoredCanonicalizedHeaders {
		ignoredHeaders[k] = v
	}
	return ignoredHeaders
}

type forceSignRoundTripper struct {
	rt                          http.RoundTripper
	keys                        Keys
	methods                     string
	host                        string
	ignoredCanonicalizedHeaders map[string]bool
}

// RoundTrip implements http.RoundTripper interface
func (srt forceSignRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if srt.shouldBeSigned(req) {
		req = s3signer.SignV2(req, srt.keys.AccessKeyID, srt.keys.SecretAccessKey, srt.ignoredCanonicalizedHeaders)
	}
	return srt.rt.RoundTrip(req)
}

func sign(req *http.Request, authHeader utils.ParsedAuthorizationHeader, newHost, accessKey, secretKey string, ignoredHeaders, v4IgnoredHeaders map[string]bool) (*http.Request, error) {
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
			return s3signer.StreamingSignV4WithIgnoredHeaders(req, accessKey, secretKey, "", authHeader.Region, authHeader.Service, dataLen, time.Now().UTC(), v4IgnoredHeaders, true), nil
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
func ForceSignDecorator(keys Keys, host, methods string, ignoredCanonicalizedHeaders map[string]bool) httphandler.Decorator {
	return func(rt http.RoundTripper) http.RoundTripper {
		return forceSignRoundTripper{rt: rt, host: host, keys: keys, methods: methods, ignoredCanonicalizedHeaders: ignoredCanonicalizedHeaders}
	}
}
