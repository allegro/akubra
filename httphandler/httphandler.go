package httphandler

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"net/http"
	"sync/atomic"

	"github.com/allegro/akubra/httphandler/config"
	"github.com/allegro/akubra/log"
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
	log.Printf("handler url %s", req.URL)
	log.Printf("url host %s, header host %s, req host %s", req.URL.Host, req.Header.Get("Host"), req.Host)

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
		log.Printf("%s", err)
		return
	}
	defer respBodyCloserFactory(resp, randomIDStr)()

	wh := w.Header()
	for k, v := range resp.Header {
		wh[k] = v
	}

	w.WriteHeader(resp.StatusCode)
	if resp.Body == nil {
		return
	}

	if _, copyErr := io.Copy(w, resp.Body); copyErr != nil {
		log.Printf("Handler.ServeHTTP Cannot send response body %s reason: %q",
			randomIDStr,
			copyErr.Error())
	} else {
		log.Printf("Handler.ServeHTTP Sent response body %s",
			randomIDStr)
	}
}

func respBodyCloserFactory(resp *http.Response, randomIDStr string) func() {
	return func() {
		if resp.Body == nil {
			log.Debugf("ResponseBody for request %s is nil - nothing to close (handler)", randomIDStr)
			return
		}
		closeErr := resp.Body.Close()
		log.Debugf("ResponseBody for request %s closed with %s error (handler)", randomIDStr, closeErr)
	}
}

func (h *Handler) validateIncomingRequest(req *http.Request) int {
	return config.RequestHeaderContentLengthValidator(*req, h.bodyMaxSize)
}

// DecorateRoundTripper applies common http.RoundTripper decorators
func DecorateRoundTripper(conf config.Client, accesslog log.Logger, healthCheckEndpoint string, rt http.RoundTripper) http.RoundTripper {
	return Decorate(
		rt,
		HeadersSuplier(conf.AdditionalRequestHeaders, conf.AdditionalResponseHeaders),
		AccessLogging(accesslog),
		OptionsHandler,
		HealthCheckHandler(healthCheckEndpoint),
	)
}

// NewHandlerWithRoundTripper returns Handler, but will not construct transport.MultiTransport by itself
func NewHandlerWithRoundTripper(roundTripper http.RoundTripper, servConfig config.Server) (http.Handler, error) {
	return &Handler{
		roundTripper:          roundTripper,
		bodyMaxSize:           servConfig.BodyMaxSize.SizeInBytes,
		maxConcurrentRequests: servConfig.MaxConcurrentRequests,
	}, nil
}
