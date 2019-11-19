package httphandler

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"io/ioutil"

	"github.com/allegro/akubra/internal/akubra/httphandler/config"
	"github.com/allegro/akubra/internal/akubra/log"
	"github.com/allegro/akubra/internal/akubra/privacy"
	"github.com/allegro/akubra/internal/akubra/utils"
)

var incorrectAuthHeader = "Incorrect auth header"

// Decorator is http.RoundTripper interface wrapper
type Decorator func(http.RoundTripper) http.RoundTripper

// AccessLogging creares Decorator with access log collector
func AccessLogging(logger log.Logger) Decorator {
	return func(rt http.RoundTripper) http.RoundTripper {
		return &loggingRoundTripper{roundTripper: rt, accessLog: logger}
	}
}

type loggingRoundTripper struct {
	roundTripper http.RoundTripper
	accessLog    log.Logger
}

func (lrt *loggingRoundTripper) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	log.Debug("Request in LoggingRoundTripper %s", utils.RequestID(req))
	timeStart := time.Now()
	resp, err = lrt.roundTripper.RoundTrip(req)

	duration := time.Since(timeStart).Seconds() * 1000
	statusCode := http.StatusServiceUnavailable

	if resp != nil {
		statusCode = resp.StatusCode
	}

	errStr := ""
	if err != nil {
		errStr = err.Error()
	}
	accessLogMessage := NewAccessLogMessage(req,
		statusCode,
		duration,
		errStr)
	jsonb, almerr := json.Marshal(accessLogMessage)
	if almerr != nil {
		log.Printf("Cannot marshal access log message %s", almerr.Error())
		return
	}
	lrt.accessLog.Printf("%s", jsonb)
	return
}

type headersSuplier struct {
	requestHeaders  config.AdditionalHeaders
	responseHeaders config.AdditionalHeaders
	roundTripper    http.RoundTripper
}

func (hs *headersSuplier) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	log.Debug("Request in headersSuplier %s", utils.RequestID(req))
	req.URL.Scheme = "http"
	for k, v := range hs.requestHeaders {
		_, ok := req.Header[k]
		if !ok {
			req.Header.Set(k, v)
		}
	}

	// While tcp host is rewritten we need to keep Host header
	// intact for sake of s3 authorization
	if strings.Contains(req.Host, ".s3.") {
		prefix := strings.Split(req.Host, ".s3.")[0]
		newhost := prefix + "." + req.URL.Host
		req.Header.Set("Host", newhost)
		req.Host = newhost
	}

	resp, err = hs.roundTripper.RoundTrip(req)

	if err != nil || resp == nil {
		return
	}
	if resp.Header == nil {
		resp.Header = http.Header{}
	}
	for k, v := range hs.responseHeaders {
		headerValue := resp.Header.Get(k)
		if headerValue == "" {
			resp.Header.Set(k, v)
		}
	}
	return
}

// HeadersSuplier creates Decorator which adds headers to request and response
func HeadersSuplier(requestHeaders, responseHeaders config.AdditionalHeaders) Decorator {
	return func(roundTripper http.RoundTripper) http.RoundTripper {
		return &headersSuplier{
			requestHeaders:  requestHeaders,
			responseHeaders: responseHeaders,
			roundTripper:    roundTripper}
	}
}

type responseHeadersStripper struct {
	headers      []string
	roundTripper http.RoundTripper
}

func (hs *responseHeadersStripper) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	log.Debug("Request in responseHeadersStripper %s", utils.RequestID(req))
	resp, err = hs.roundTripper.RoundTrip(req)
	if err != nil || resp == nil {
		return
	}
	for _, header := range hs.headers {
		resp.Header.Del(header)
	}
	return
}

// ResponseHeadersStripper creates Decorator which strips the Akubra specific headers
func ResponseHeadersStripper(headersToStrip []string) Decorator {
	return func(roundTripper http.RoundTripper) http.RoundTripper {
		return &responseHeadersStripper{
			headers:      headersToStrip,
			roundTripper: roundTripper}
	}
}

//PrivacyContextSupplier creates Decorator which supplies the request with security context
func PrivacyContextSupplier(supplier privacy.ContextSupplier) Decorator {
	return func(roundTripper http.RoundTripper) http.RoundTripper {
		return privacy.NewPrivacyContextSupplierRoundTripper(roundTripper, supplier)
	}
}

//PrivacyFilterChain creates Decorator checks for any privacy violations
func PrivacyFilterChain(shouldDrop bool, violationErrorCode int, chain privacy.Chain) Decorator {
	return func(roundTripper http.RoundTripper) http.RoundTripper {
		if violationErrorCode == 0 {
			violationErrorCode = http.StatusForbidden
		}
		return privacy.NewChainRoundTripper(shouldDrop, violationErrorCode, chain, roundTripper)
	}
}

type optionsHandler struct {
	roundTripper http.RoundTripper
}

func (os optionsHandler) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	log.Debug("Request in optionsHandler %s", utils.RequestID(req))
	isOptions := false
	if req.Method == "OPTIONS" {
		req.Method = "HEAD"
		isOptions = true
	}

	resp, err = os.roundTripper.RoundTrip(req)
	if resp != nil && isOptions {
		resp.Header.Set("Content-Length", "0")
	}

	return
}

// OptionsHandler changes OPTIONS method it to HEAD and pass it to
// decorated http.RoundTripper, also clears response content-length header
func OptionsHandler(roundTripper http.RoundTripper) http.RoundTripper {
	return optionsHandler{roundTripper: roundTripper}
}

type statusHandler struct {
	healthCheckEndpoint string
	roundTripper        http.RoundTripper
}

func (sh statusHandler) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	log.Debug("Request in status Handler %s", utils.RequestID(req))
	if strings.ToLower(req.URL.Path) == sh.healthCheckEndpoint {
		resp := makeResponse(req, http.StatusOK, "OK", "text/plain")
		return resp, nil
	}
	return sh.roundTripper.RoundTrip(req)
}

func makeResponse(req *http.Request, status int, body string, contentType string) *http.Response {
	resp := &http.Response{}
	bodyContent := body
	resp.Body = ioutil.NopCloser(strings.NewReader(bodyContent))
	resp.ContentLength = int64(len(bodyContent))
	resp.Header = make(http.Header)
	resp.Header.Set("Cache-Control", "no-cache, no-store")
	resp.Header.Set("Content-Type", contentType)
	resp.StatusCode = status
	return resp
}

// HealthCheckHandler serving health check endpoint
func HealthCheckHandler(healthCheckEndpoint string) Decorator {
	return func(roundTripper http.RoundTripper) http.RoundTripper {
		return &statusHandler{
			healthCheckEndpoint: healthCheckEndpoint,
			roundTripper:        roundTripper,
		}
	}
}

// AuthHeaderContextSuplementer adds utils.ParsedAuthorizationHeader to request
// context value
func AuthHeaderContextSuplementer() Decorator {
	return func(roundTripper http.RoundTripper) http.RoundTripper {
		return &authHeaderContextSuplementer{
			roundTripper: roundTripper,
		}
	}
}

type authHeaderContextSuplementer struct {
	roundTripper http.RoundTripper
}

func (authHeaderRT *authHeaderContextSuplementer) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	httpAuthHeader := req.Header.Get("Authorization")
	if httpAuthHeader != "" {
		authHeader, err := utils.ParseAuthorizationHeader(httpAuthHeader)
		if err != nil {
			log.Debugf("failed to parse auth header for req %s: %q", utils.RequestID(req), err)
			return makeResponse(req, http.StatusBadRequest, incorrectAuthHeader, "text/plain"), nil
		}
		reqCtx := context.WithValue(req.Context(), AuthHeader, &authHeader)
		req = req.WithContext(reqCtx)
	}
	return authHeaderRT.RoundTrip(req)
}

// RequestLimiter limits number of concurrent requests
func RequestLimiter(maxConcurrentRequests int32) Decorator {
	return func(roundTripper http.RoundTripper) http.RoundTripper {
		return &requestLimitRoundTripper{
			roundTripper:          roundTripper,
			maxConcurrentRequests: maxConcurrentRequests,
			runningRequestCount:   0,
		}
	}
}

type requestLimitRoundTripper struct {
	roundTripper          http.RoundTripper
	runningRequestCount   int32
	maxConcurrentRequests int32
}

func (rlrt *requestLimitRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	canServe := true
	if atomic.AddInt32(&rlrt.runningRequestCount, 1) > rlrt.maxConcurrentRequests {
		canServe = false
	}
	defer atomic.AddInt32(&rlrt.runningRequestCount, -1)
	if !canServe {
		log.Printf("Rejected request from %s - too many other requests in progress.", req.Host)
		return makeResponse(req, http.StatusServiceUnavailable, "Too many requests in progress.", "text/plain"), nil
	}
	return rlrt.roundTripper.RoundTrip(req)
}

// BodySizeLimitter rejects requests with to large body size
func BodySizeLimitter(bodySizeLimit int64) Decorator {
	return func(roundTripper http.RoundTripper) http.RoundTripper {
		return &bodySizeLimitter{
			roundTripper:  roundTripper,
			bodySizeLimit: bodySizeLimit,
		}
	}
}

type bodySizeLimitter struct {
	roundTripper  http.RoundTripper
	bodySizeLimit int64
}

func (sizeLimitter *bodySizeLimitter) RoundTrip(req *http.Request) (*http.Response, error) {
	validationCode := sizeLimitter.validateIncomingRequest(req)
	if validationCode > 0 {
		log.Printf("Rejected invalid incoming request from %s, code %d", req.RemoteAddr, validationCode)
		return makeResponse(req, validationCode, "Too large body size.", "text/plain"), nil
	}
	return sizeLimitter.roundTripper.RoundTrip(req)
}

func (sizeLimitter *bodySizeLimitter) validateIncomingRequest(req *http.Request) int {
	return config.RequestHeaderContentLengthValidator(*req, sizeLimitter.bodySizeLimit)
}

// Decorate returns http.Roundtripper wraped with all passed decorators
func Decorate(roundTripper http.RoundTripper, decorators ...Decorator) http.RoundTripper {

	for _, dec := range decorators {
		roundTripper = dec(roundTripper)
	}
	return roundTripper
}
