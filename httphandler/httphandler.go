package httphandler

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/allegro/akubra/config"
	"github.com/allegro/akubra/log"
)

const (
	defaultMaxIdleConnsPerHost   = 100
	defaultResponseHeaderTimeout = 5 * time.Second
)

// Handler implements http.Handler interface
type Handler struct {
	roundTripper http.RoundTripper
	bodyMaxSize  int64
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {

	randomID := make([]byte, 12)
	_, err := rand.Read(randomID)
	if err != nil {
		randomID = []byte("notrandomid")
	}

	validationCode := h.validateIncomingRequest(req)
	if validationCode > 0 {
		log.Printf("Rejected invalid incoming request from %s, code %d", req.RemoteAddr, validationCode)
		w.WriteHeader(validationCode)
		return
	}

	randomIDStr := hex.EncodeToString(randomID)
	randomIDContext := context.WithValue(req.Context(), log.ContextreqIDKey, randomIDStr)
	log.Debugf("Request id %s", randomIDStr)
	resp, err := h.roundTripper.RoundTrip(req.WithContext(randomIDContext))

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	wh := w.Header()
	for k, v := range resp.Header {
		wh[k] = v
	}

	w.WriteHeader(resp.StatusCode)
	_, copyErr := io.Copy(w, resp.Body)

	defer func() {
		if copyErr != nil {
			log.Printf("Cannot send response body reason: %q",
				copyErr.Error())
		}
	}()

	defer func() {
		closeErr := resp.Body.Close()
		if closeErr != nil {
			log.Printf("Cannot send response body reason: %q",
				closeErr.Error())
		}
	}()
}

func (h *Handler) validateIncomingRequest(req *http.Request) int {
	var contentLength int64
	contentLengthHeader := req.Header.Get("Content-Length")
	if contentLengthHeader != "" {
		var err error
		contentLength, err = strconv.ParseInt(contentLengthHeader, 10, 64)
		if err != nil {
			return http.StatusBadRequest
		}
	}
	if contentLength > h.bodyMaxSize || req.ContentLength > h.bodyMaxSize {
		return http.StatusRequestEntityTooLarge
	}
	return 0
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
	)
}

// NewHandlerWithRoundTripper returns Handler, but will not construct transport.MultiTransport by itself
func NewHandlerWithRoundTripper(roundTripper http.RoundTripper, bodyMaxSize int64) (http.Handler, error) {
	return &Handler{
		roundTripper: roundTripper,
		bodyMaxSize:  bodyMaxSize,
	}, nil
}
