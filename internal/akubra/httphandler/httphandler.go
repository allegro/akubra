package httphandler

import (
	"context"
	"fmt"
	"github.com/allegro/akubra/internal/akubra/utils"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/allegro/akubra/internal/akubra/metrics"

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

func randomStr(length int) string {
	return uuid.Must(uuid.NewV4()).String()[:length]
}

// Handler implements http.Handler interface
type Handler struct {
	roundTripper http.RoundTripper
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	since := time.Now()
	randomIDStr := randomStr(36)
	req, err := prepareRequestWithContextValues(req, randomIDStr)
	if err != nil {
		log.Debugf("failed to parse auth header for req %s: %q", randomIDStr, err)
		w.WriteHeader(http.StatusBadRequest)
		_, err := w.Write([]byte(incorrectAuthHeader))
		if err != nil {
			log.Debug(err)
		}
		return
	}

	req.Header.Del("Expect")


	resp, err := h.roundTripper.RoundTrip(req)
	defer sendStats(req, resp, err, since)

	if err != nil || resp == nil {
		w.WriteHeader(http.StatusInternalServerError)
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
	}
}

func sendStats(req *http.Request, resp *http.Response, err error, since time.Time) {
	metrics.UpdateSince("reqs.global.all", since)
	if err != nil {
		metrics.UpdateSince("reqs.global.err", since)
	}
	if resp != nil {
		name := fmt.Sprintf("reqs.global.status_%d", resp.StatusCode)
		metrics.UpdateSince(name, since)
	} else {
		name := fmt.Sprintf("reqs.global.status_%d", http.StatusInternalServerError)
		metrics.UpdateSince(name, since)
	}
	if req != nil {
		methodName := fmt.Sprintf("reqs.global.method_%s", req.Method)
		metrics.UpdateSince(methodName, since)
	}
}

func prepareRequestWithContextValues(req *http.Request, requestID string) (*http.Request, error) {
	reqHost, _, err := net.SplitHostPort(req.Host)
	if err != nil {
		reqHost = req.Host
	}
	utils.SetRequestProcessingMetadata(req, "requestID", requestID)
	reqCtx := context.WithValue(req.Context(), log.ContextreqIDKey, requestID)
	httpAuthHeader := req.Header.Get("Authorization")
	if httpAuthHeader != "" {
		authHeader, err := utils.ParseAuthorizationHeader(httpAuthHeader)
		if err != nil {
			return req, err
		}
		reqCtx = context.WithValue(reqCtx, AuthHeader, &authHeader)
	}
	return req.WithContext(context.WithValue(reqCtx, Domain, reqHost)), nil
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

// DecorateRoundTripper applies common http.RoundTripper decorators
func DecorateRoundTripper(conf config.Client, servConfig config.Server, accesslog log.Logger, healthCheckEndpoint string, rt http.RoundTripper) http.RoundTripper {
	return Decorate(
		rt,
		RequestLimiter(servConfig.MaxConcurrentRequests),
		BodySizeLimitter(servConfig.BodyMaxSize.SizeInBytes),
		HeadersSuplier(conf.AdditionalRequestHeaders, conf.AdditionalResponseHeaders),
		OptionsHandler,
		HealthCheckHandler(healthCheckEndpoint),
	)
}

// NewHandlerWithRoundTripper returns Handler, but will not construct transport.MultiTransport by itself
func NewHandlerWithRoundTripper(roundTripper http.RoundTripper, servConfig config.Server) (http.Handler, error) {
	return &Handler{
		roundTripper: roundTripper,
	}, nil
}
