package auth

import (
	"github.com/allegro/akubra/utils"
	"net/http"
	"strings"

	"github.com/allegro/akubra/crdstore"
	"github.com/allegro/akubra/httphandler"
	"github.com/allegro/akubra/log"
	"github.com/bnogas/minio-go/pkg/s3signer"
)



// DoesSignMatch - Verify authorization header with calculated header
// returns true if matches, false otherwise. if error is not nil then it is always false
func DoesSignMatch(r *http.Request, cred Keys) utils.APIErrorCode {
	gotAuth := r.Header.Get("Authorization")
	if gotAuth == "" {
		return utils.ErrAuthHeaderEmpty
	}
	authHeader, err := utils.ParseAuthorizationHeader(gotAuth)
	if err != nil {
		return utils.ErrIncorrectAuthHeader
	}

	switch authHeader.Version {
	case utils.SignV2Algorithm:
		result, err := s3signer.VerifyV2(*r, cred.SecretAccessKey)
		if err != nil {
			reqID := r.Context().Value(log.ContextreqIDKey)
			log.Printf("Error while verifying V2 Signature for request %s: %s", reqID, err)
		}
		if !result {
			return utils.ErrSignatureDoesNotMatch
		}
	case utils.SignV4Algorithm:
		result, err := s3signer.VerifyV4(*r, cred.SecretAccessKey)
		if err != nil {
			reqID := r.Context().Value(log.ContextreqIDKey)
			log.Printf("Error while verifying V4 Signature for request %s: %s", reqID, err)
		}
		if !result {
			return utils.ErrSignatureDoesNotMatch
		}
	default:
		return utils.ErrUnsupportedSignatureVersion
	}

	return utils.ErrNone
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
	if DoesSignMatch(req, art.keys) == utils.ErrNone {
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
	host    string
}

// RoundTrip implements http.RoundTripper interface
func (srt signRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	authHeader, err := utils.ParseAuthorizationHeader(req.Header.Get("Authorization"))
	if err != nil {
		return &http.Response{StatusCode: http.StatusBadRequest, Request: req}, err
	}

	req = s3signer.SignV2(*req, srt.keys.AccessKeyID, srt.keys.SecretAccessKey)
	switch authHeader.Version {
	case utils.SignV2Algorithm:
		req = s3signer.SignV2(*req, srt.keys.AccessKeyID, srt.keys.SecretAccessKey)
	case utils.SignV4Algorithm:
		req.URL.Host = srt.host
		req = s3signer.SignV4(*req, srt.keys.AccessKeyID, srt.keys.SecretAccessKey, "", srt.region)
	}
	return srt.rt.RoundTrip(req)
}

// RoundTrip implements http.RoundTripper interface
func (srt signAuthServiceRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	authHeader, err := utils.ParseAuthorizationHeader(req.Header.Get("Authorization"))
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
	if DoesSignMatch(req, Keys{AccessKeyID: csd.AccessKey, SecretAccessKey: csd.SecretKey}) != utils.ErrNone {
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
	case utils.SignV2Algorithm:
		req = s3signer.SignV2(*req, csd.AccessKey, csd.SecretKey)
	case utils.SignV4Algorithm:
		req = s3signer.SignV4(*req, csd.AccessKey, csd.SecretKey, "", "")
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
func SignAuthServiceDecorator(backend, endpoint, host string) httphandler.Decorator {
	return func(rt http.RoundTripper) http.RoundTripper {
		credentialsStore, err := crdstore.GetInstance(endpoint)
		if err != nil {
			log.Fatalf("error CredentialsStore `%s` is not defined", endpoint)
		}
		return signAuthServiceRoundTripper{rt: rt, backend: backend, host: host, crd: credentialsStore}
	}
}

type forceSignRoundTripper struct {
	rt      http.RoundTripper
	keys    Keys
	methods string
	host    string
}

// RoundTrip implements http.RoundTripper interface
func (srt forceSignRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if srt.shouldBeSigned(req) {
		header := make(http.Header, len(req.Header))
		for k, v := range req.Header {
			header[k] = v
		}
		req.Header = header
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
func ForceSignDecorator(keys Keys, host, methods string) httphandler.Decorator {
	return func(rt http.RoundTripper) http.RoundTripper {
		return forceSignRoundTripper{rt: rt, host: host, keys: keys, methods: methods}
	}
}
