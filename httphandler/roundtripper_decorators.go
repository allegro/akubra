package httphandler

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"io/ioutil"

	"github.com/allegro/akubra/log"
	shardingconfig "github.com/allegro/akubra/sharding/config"
)

// Decorator is http.RoundTripper interface wrapper
type Decorator func(http.RoundTripper) http.RoundTripper

type loggingRoundTripper struct {
	roundTripper http.RoundTripper
	accessLog    log.Logger
}

func (lrt *loggingRoundTripper) RoundTrip(req *http.Request) (resp *http.Response, err error) {

	timeStart := time.Now()
	resp, err = lrt.roundTripper.RoundTrip(req)

	duration := time.Since(timeStart).Seconds()
	statusCode := http.StatusServiceUnavailable

	if resp != nil {
		statusCode = resp.StatusCode
	}

	errStr := ""
	if err != nil {
		errStr = err.Error()
	}
	accessLogMessage := NewAccessLogMessage(*req,
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

// AccessLogging creares Decorator with access log collector
func AccessLogging(logger log.Logger) Decorator {
	return func(rt http.RoundTripper) http.RoundTripper {
		return &loggingRoundTripper{roundTripper: rt, accessLog: logger}
	}
}

type headersSuplier struct {
	requestHeaders  shardingconfig.AdditionalHeaders
	responseHeaders shardingconfig.AdditionalHeaders
	roundTripper    http.RoundTripper
}

func (hs *headersSuplier) RoundTrip(req *http.Request) (resp *http.Response, err error) {

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

	if err != nil {
		return
	}

	for k, v := range hs.responseHeaders {
		_, ok := resp.Header[k]
		if !ok {
			resp.Header.Set(k, v)
		}
	}
	return
}

// HeadersSuplier creates Decorator which adds headers to request and response
func HeadersSuplier(requestHeaders, responseHeaders shardingconfig.AdditionalHeaders) Decorator {
	return func(roundTripper http.RoundTripper) http.RoundTripper {
		return &headersSuplier{
			requestHeaders:  requestHeaders,
			responseHeaders: responseHeaders,
			roundTripper:    roundTripper}
	}
}

type optionsHandler struct {
	roundTripper http.RoundTripper
}

func (os optionsHandler) RoundTrip(req *http.Request) (resp *http.Response, err error) {
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

	if strings.ToLower(req.URL.Path) == sh.healthCheckEndpoint {
		resp := &http.Response{}
		bodyContent := "OK"
		resp.Body = ioutil.NopCloser(strings.NewReader(bodyContent))
		resp.ContentLength = int64(len(bodyContent))
		resp.Header = make(http.Header, 0)
		resp.Header.Set("Cache-Control", "no-cache, no-store")
		resp.Header.Set("Content-Type", "text/plain")
		resp.StatusCode = http.StatusOK
		return resp, nil
	}
	return sh.roundTripper.RoundTrip(req)
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

// Decorate returns http.Roundtripper wraped with all passed decorators
func Decorate(roundTripper http.RoundTripper, decorators ...Decorator) http.RoundTripper {

	for _, dec := range decorators {
		roundTripper = dec(roundTripper)
	}
	return roundTripper
}
