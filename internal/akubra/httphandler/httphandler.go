package httphandler

import (
	"context"
	"github.com/allegro/akubra/internal/akubra/utils"
	"io"
	"net"
	"net/http"
	"sync/atomic"

	"github.com/allegro/akubra/internal/akubra/httphandler/config"
	"github.com/allegro/akubra/internal/akubra/log"
	"github.com/gofrs/uuid"
)

const (
	//Domain is a constant used to put/get domain's name to/from request's context
	Domain = log.ContextKey("Domain")
	//AuthHeader is a constant used to put/get domain's name to/from request's context
	AuthHeader = log.ContextKey("AuthHeader")
)

var incorrectAuthHeader = []byte("Incorrect auth header")

func randomStr(length int) string {
	return uuid.Must(uuid.NewV4()).String()[:length]
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

	randomIDStr := randomStr(36)
	log.Printf("reqid %s url host %s, header host %s, req host %s", randomIDStr, req.URL.Host, req.Header.Get("Host"), req.Host)

	if atomic.AddInt32(&h.runningRequestCount, 1) > h.maxConcurrentRequests {
		canServe = false
	}
	defer atomic.AddInt32(&h.runningRequestCount, -1)
	if !canServe {
		log.Printf("Rejected request from %s - too many other requests in progress.", req.Host)
		http.Error(w, "Too many requests in progress.", http.StatusServiceUnavailable)
		return
	}

	validationCode := h.validateIncomingRequest(req)
	if validationCode > 0 {
		log.Printf("Rejected invalid incoming request from %s, code %d", req.RemoteAddr, validationCode)
		w.WriteHeader(validationCode)
		return
	}

	reqHost, _, err := net.SplitHostPort(req.Host)
	if err != nil {
		reqHost = req.Host
	}

	reqCtx := context.WithValue(req.Context(), log.ContextreqIDKey, randomIDStr)
	reqCtx = context.WithValue(reqCtx, Domain, reqHost)

	httpAuthHeader := req.Header.Get("Authorization")
	if httpAuthHeader != "" {
		authHeader, err := utils.ParseAuthorizationHeader(httpAuthHeader)
		if err != nil {
			log.Debugf("failed to parse auth header for req %s: %q", randomIDStr, err)
			w.WriteHeader(http.StatusBadRequest)
			_, err := w.Write(incorrectAuthHeader)
			if err != nil {
				log.Debug(err)
			}
			return
		}
		reqCtx = context.WithValue(reqCtx, AuthHeader, &authHeader)
	}


	log.Debugf("Request id %s, domain %s", randomIDStr, reqHost)

	req.Header.Del("Expect")
	resp, err := h.roundTripper.RoundTrip(req.WithContext(reqCtx))

	if err != nil || resp == nil {
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
		if resp == nil {
			return
		}
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
