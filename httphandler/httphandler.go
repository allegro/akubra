package httphandler

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/allegro/akubra/config"
	"github.com/allegro/akubra/log"
)

const (
	defaultMaxIdleConnsPerHost   = 100
	defaultResponseHeaderTimeout = 5 * time.Second
)

func randomStr(length int) string {
	randomID := make([]byte, length)
	_, err := rand.Read(randomID)
	if err != nil {
		randomID = []byte("notrandomid")
	}
	return hex.EncodeToString(randomID)
}

// Handler implements http.Handler interface
type Handler struct {
	roundTripper          http.RoundTripper
	bodyMaxSize           int64
	maxConcurrentRequests int32
	runningRequestCount   int32
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	canServe := true
	if atomic.AddInt32(&h.runningRequestCount, 1) > h.maxConcurrentRequests {
		canServe = false
	}
	defer atomic.AddInt32(&h.runningRequestCount, -1)
	if !canServe {
		log.Printf("Rejected request from %s - too many other requests in progress.", req.Host)
		http.Error(w, "Too many requests in progress.", http.StatusServiceUnavailable)
		return
	}

	randomIDStr := randomStr(12)
	validationCode := h.validateIncomingRequest(req)
	if validationCode > 0 {
		log.Printf("Rejected invalid incoming request from %s, code %d", req.RemoteAddr, validationCode)
		w.WriteHeader(validationCode)
		return
	}

	randomIDContext := context.WithValue(req.Context(), log.ContextreqIDKey, randomIDStr)
	log.Debugf("Request id %s", randomIDStr)

	resp, err := h.roundTripper.RoundTrip(req.WithContext(randomIDContext))

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer func() {
		closeErr := resp.Body.Close()
		if closeErr != nil {
			log.Printf("Cannot send response body reason: %q",
				closeErr.Error())
		}
	}()

	wh := w.Header()
	for k, v := range resp.Header {
		wh[k] = v
	}

	w.WriteHeader(resp.StatusCode)
	if _, copyErr := io.Copy(w, resp.Body); copyErr != nil {
		log.Printf("Cannot send response body reason: %q",
			copyErr.Error())
	}
}

func (h *Handler) validateIncomingRequest(req *http.Request) int {
	return config.RequestHeaderContentLengthValidator(*req, h.bodyMaxSize)
}

// ConfigureHTTPTransport returns http.Transport with customized dialer,
// MaxIdleConnsPerHost and DisableKeepAlives
func ConfigureHTTPTransport(conf config.Config) (*http.Transport, error) {
	maxIdleConnsPerHost := defaultMaxIdleConnsPerHost
	responseHeaderTimeout := defaultResponseHeaderTimeout

	if conf.MaxIdleConnsPerHost != 0 {
		maxIdleConnsPerHost = conf.MaxIdleConnsPerHost
	}

	if conf.ResponseHeaderTimeout.Duration != 0 {
		responseHeaderTimeout = conf.ResponseHeaderTimeout.Duration
	}

	httpTransport := &http.Transport{
		MaxIdleConns:          conf.MaxIdleConns,
		MaxIdleConnsPerHost:   maxIdleConnsPerHost,
		IdleConnTimeout:       conf.IdleConnTimeout.Duration,
		ResponseHeaderTimeout: responseHeaderTimeout,
		DisableKeepAlives:     conf.DisableKeepAlives,
	}

	return httpTransport, nil
}

// DecorateRoundTripper applies common http.RoundTripper decorators
func DecorateRoundTripper(conf config.Config, rt http.RoundTripper) http.RoundTripper {
	return Decorate(
		rt,
		HeadersSuplier(conf.AdditionalRequestHeaders, conf.AdditionalResponseHeaders),
		AccessLogging(conf.Accesslog),
		OptionsHandler,
		HealthCheckHandler(conf.HealthCheckEndpoint),
	)
}

// NewHandlerWithRoundTripper returns Handler, but will not construct transport.MultiTransport by itself
func NewHandlerWithRoundTripper(roundTripper http.RoundTripper, bodyMaxSize int64, maxConcurrentRequests int32) (http.Handler, error) {
	return &Handler{
		roundTripper:          roundTripper,
		bodyMaxSize:           bodyMaxSize,
		maxConcurrentRequests: maxConcurrentRequests,
	}, nil
}
