package auth

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/allegro/akubra/crdstore"
	"github.com/allegro/akubra/httphandler"
	"github.com/allegro/akubra/log"
)

// APIErrorCode type of error status.
type APIErrorCode int

// Error codes, non exhaustive list - http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
const (
	ErrAuthHeaderEmpty APIErrorCode = iota
	ErrSignatureDoesNotMatch
	ErrNone
)

// Whitelist resource list that will be used in query string for signature-V2 calculation.
var resourceList = []string{
	"acl",
	"delete",
	"lifecycle",
	"location",
	"logging",
	"notification",
	"partNumber",
	"policy",
	"requestPayment",
	"torrent",
	"uploadId",
	"uploads",
	"versionId",
	"versioning",
	"versions",
	"website",
}

// Signature and API related constants.
const (
	signV2Algorithm = "AWS"
)

// const (
// 	// S3FixedKey points s3 decorator with global s3 keys pair
// 	S3FixedKey = "S3"
// )

// Return canonical headers.
func canonicalizedAmzHeadersV2(headers http.Header) string {
	var keys []string
	keyval := make(map[string]string)
	for key := range headers {
		lkey := strings.ToLower(key)
		if !strings.HasPrefix(lkey, "x-amz-") {
			continue
		}
		keys = append(keys, lkey)
		keyval[lkey] = strings.Join(headers[key], ",")
	}
	sort.Strings(keys)
	var canonicalHeaders []string
	for _, key := range keys {
		canonicalHeaders = append(canonicalHeaders, key+":"+keyval[key])
	}
	return strings.Join(canonicalHeaders, "\n")
}

// Return canonical resource string.
func canonicalizedResourceV2(encodedPath string, encodedQuery string) string {
	queries := strings.Split(encodedQuery, "&")
	keyval := make(map[string]string)
	for _, query := range queries {
		key := query
		val := ""
		index := strings.Index(query, "=")
		if index != -1 {
			key = query[:index]
			val = query[index+1:]
		}
		keyval[key] = val
	}
	var canonicalQueries []string
	for _, key := range resourceList {
		val, ok := keyval[key]
		if !ok {
			continue
		}
		if val == "" {
			canonicalQueries = append(canonicalQueries, key)
			continue
		}
		canonicalQueries = append(canonicalQueries, key+"="+val)
	}
	if len(canonicalQueries) == 0 {
		return encodedPath
	}
	// the queries will be already sorted as resourceList is sorted.
	return encodedPath + "?" + strings.Join(canonicalQueries, "&")
}

// Return string to sign for authz header calculation.
func signV2STS(method string, encodedResource string, encodedQuery string, headers http.Header) string {
	canonicalHeaders := canonicalizedAmzHeadersV2(headers)
	if len(canonicalHeaders) > 0 {
		canonicalHeaders += "\n"
	}

	// From the Amazon docs:
	//
	// StringToSign = HTTP-Verb + "\n" +
	// 	 Content-Md5 + "\n" +
	//	 Content-Type + "\n" +
	//	 Date + "\n" +
	//	 CanonicalizedProtocolHeaders +
	//	 CanonicalizedResource;
	stringToSign := strings.Join([]string{
		method,
		headers.Get("Content-MD5"),
		headers.Get("Content-Type"),
		headers.Get("Date"),
		canonicalHeaders,
	}, "\n") + canonicalizedResourceV2(encodedResource, encodedQuery)

	return stringToSign
}

// Keys user credentials
type Keys struct {
	AccessKeyID     string `json:"access-key" yaml:"AccessKey"`
	SecretAccessKey string `json:"secret-key" yaml:"Secret"`
}

// Return signature-v2 authrization header.
func signatureV2(method string, encodedResource string, encodedQuery string, headers http.Header, cred Keys) string {
	stringToSign := signV2STS(method, encodedResource, encodedQuery, headers)
	hm := hmac.New(sha1.New, []byte(cred.SecretAccessKey))
	_, err := hm.Write([]byte(stringToSign))
	if err != nil {
		log.Printf("Cannot write to hmac io.Writter, %q", err)
	}
	signature := base64.StdEncoding.EncodeToString(hm.Sum(nil))
	return fmt.Sprintf("%s %s:%s", signV2Algorithm, cred.AccessKeyID, signature)
}

// Authorization = "AWS" + " " + AWSAccessKeyId + ":" + Signature;
// Signature = Base64( HMAC-SHA1( YourSecretAccessKeyID, UTF-8-Encoding-Of( StringToSign ) ) );
//
// StringToSign = HTTP-Verb + "\n" +
//  	Content-Md5 + "\n" +
//  	Content-Type + "\n" +
//  	Date + "\n" +
//  	CanonicalizedProtocolHeaders +
//  	CanonicalizedResource;
//
// CanonicalizedResource = [ "/" + Bucket ] +
//  	<HTTP-Request-URI, from the protocol name up to the query string> +
//  	[ subresource, if present. For example "?acl", "?location", "?logging", or "?torrent"];
//
// CanonicalizedProtocolHeaders = <described below>

// DoesSignV2Match - Verify authorization header with calculated header in accordance with
//     - http://docs.aws.amazon.com/AmazonS3/latest/dev/auth-request-sig-v2.html
// returns true if matches, false otherwise. if error is not nil then it is always false
func DoesSignV2Match(r *http.Request, cred Keys) APIErrorCode {
	gotAuth := r.Header.Get("Authorization")
	if gotAuth == "" {
		return ErrAuthHeaderEmpty
	}

	// url.RawPath will be valid if path has any encoded characters, if not it will
	// be empty - in which case we need to consider url.Path (bug in net/http?)
	encodedResource := r.URL.RawPath
	encodedQuery := r.URL.RawQuery
	if encodedResource == "" {
		splits := strings.Split(r.URL.Path, "?")
		if len(splits) > 0 {
			encodedResource = splits[0]
		}
	}

	expectedAuth := signatureV2(r.Method, encodedResource, encodedQuery, r.Header, cred)
	if gotAuth != expectedAuth {
		return ErrSignatureDoesNotMatch
	}

	return ErrNone
}

// SignRequestV2 with authorization header
func SignRequestV2(r *http.Request, cred Keys) {
	r.Header.Del("Authorization")

	if r.Header.Get("Date") == "" {
		r.Header.Set("Date", time.Now().UTC().Format(time.RFC1123))
	}

	// url.RawPath will be valid if path has any encoded characters, if not it will
	// be empty - in which case we need to consider url.Path (bug in net/http?)
	encodedResource := r.URL.RawPath
	encodedQuery := r.URL.RawQuery
	if encodedResource == "" {
		splits := strings.Split(r.URL.Path, "?")
		if len(splits) > 0 {
			encodedResource = splits[0]
		}
	}

	sig := signatureV2(r.Method, encodedResource, encodedQuery, r.Header, cred)
	r.Header.Set("Authorization", sig)
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
	if DoesSignV2Match(req, art.keys) == ErrNone {
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

type signRoundTripper struct {
	rt   http.RoundTripper
	keys Keys
}

type signAuthServiceRoundTripper struct {
	rt      http.RoundTripper
	crd     *crdstore.CredentialsStore
	backend string
}

// RoundTrip implements http.RoundTripper interface
func (srt signRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {

	SignRequestV2(req, srt.keys)
	return srt.rt.RoundTrip(req)
}

// extractAccessKey - extract accessKey from S3 authorization header
func extractAccessKey(authorizationHeader string) (accessKey string, err error) {
	accessKey = strings.Trim(authorizationHeader, " ")
	start := strings.IndexAny(accessKey, " ")
	end := strings.IndexAny(accessKey, ":")
	if end <= start || start <= -1 {
		return "", fmt.Errorf("cannot find AWS AccessKey in request")
	}
	accessKey = accessKey[start+1 : end]
	return
}

// RoundTrip implements http.RoundTripper interface
func (srt signAuthServiceRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	accessKey, err := extractAccessKey(req.Header.Get("Authorization"))
	if err != nil {
		return &http.Response{StatusCode: http.StatusBadRequest, Request: req}, err
	}
	csd, err := srt.crd.Get(accessKey, "akubra")
	if err == crdstore.ErrCredentialsNotFound {
		return &http.Response{StatusCode: http.StatusForbidden, Request: req}, err
	}
	if err != nil {
		return &http.Response{StatusCode: http.StatusInternalServerError, Request: req}, err
	}
	if DoesSignV2Match(req, Keys{AccessKeyID: csd.AccessKey, SecretAccessKey: csd.SecretKey}) != ErrNone {
		return &http.Response{StatusCode: http.StatusForbidden, Request: req}, err
	}

	csd, err = srt.crd.Get(accessKey, srt.backend)
	if err == crdstore.ErrCredentialsNotFound {
		return &http.Response{StatusCode: http.StatusForbidden, Request: req}, err
	}
	if err != nil {
		return &http.Response{StatusCode: http.StatusInternalServerError, Request: req}, err
	}

	keys := Keys{csd.AccessKey, csd.SecretKey}
	SignRequestV2(req, keys)
	return srt.rt.RoundTrip(req)
}

// SignDecorator will recompute auth headers for new Key
func SignDecorator(keys Keys) httphandler.Decorator {
	return func(rt http.RoundTripper) http.RoundTripper {
		return signRoundTripper{rt: rt, keys: keys}
	}
}

// SignAuthServiceDecorator will compute
func SignAuthServiceDecorator(backend, endpoint string) httphandler.Decorator {
	return func(rt http.RoundTripper) http.RoundTripper {
		credentialsStore, err := crdstore.GetInstance(endpoint)
		if err != nil {
			log.Fatalf("error CredentialsStore `%s` is not defined", endpoint)
		}
		return signAuthServiceRoundTripper{rt: rt, backend: backend, crd: credentialsStore}
	}
}
