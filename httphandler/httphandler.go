package httphandler

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/allegro/akubra/config"
	"github.com/allegro/akubra/dial"
	"github.com/allegro/akubra/transport"
)

//Handler implements http.Handler interface
type Handler struct {
	config       config.Config
	roundTripper http.RoundTripper
	mainLog      *log.Logger
	accessLog    *log.Logger
}

func (h *Handler) closeBadRequest(w http.ResponseWriter) {

	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "webserver doesn't support hijacking", http.StatusInternalServerError)
		return
	}

	conn, _, err := hj.Hijack()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	closeErr := conn.Close()
	if closeErr != nil {
		h.mainLog.Println(closeErr.Error())
		return
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	resp, err := h.roundTripper.RoundTrip(req)

	if err != nil {
		h.closeBadRequest(w)
		w.WriteHeader(http.StatusBadRequest)
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
			h.mainLog.Printf("Cannot send response body reason: %q",
				copyErr.Error())
		}
	}()

	defer func() {
		closeErr := resp.Body.Close()
		if closeErr != nil {
			h.mainLog.Printf("Cannot send response body reason: %q",
				closeErr.Error())
		}
	}()
}

//ConfigureHTTPTransport returns http.Transport with customized dialer,
//MaxIdleConnsPerHost and DisableKeepAlives
func ConfigureHTTPTransport(conf config.Config) *http.Transport {
	connDuration, err := time.ParseDuration(conf.ConnectionTimeout)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	var dialer *dial.LimitDialer

	dialer = dial.NewLimitDialer(conf.ConnLimit, connDuration, connDuration)
	if conf.MaintainedBackend.URL != nil {
		// maintainedURL := conf.MaintainedBackend.(url.URL)
		dialer.DropEndpoint(*conf.MaintainedBackend.URL)
	}

	httpTransport := &http.Transport{
		Dial:                dialer.Dial,
		DisableKeepAlives:   conf.KeepAlive,
		MaxIdleConnsPerHost: int(conf.ConnLimit)}

	return httpTransport
}

//NewMultipleResponseHandler returns a function for a later use in transport.MultiTransport
func NewMultipleResponseHandler(conf config.Config) transport.MultipleResponsesHandler {
	rh := responseMerger{
		conf.Synclog,
		conf.Mainlog,
		conf.SyncLogMethodsSet,
	}
	return rh.handleResponses
}

//DecorateRoundTripper applies common http.RoundTripper decorators
func DecorateRoundTripper(conf config.Config, rt http.RoundTripper) http.RoundTripper {
	return Decorate(
		rt,
		HeadersSuplier(conf.AdditionalRequestHeaders, conf.AdditionalResponseHeaders),
		AccessLogging(conf.Accesslog),
		OptionsHandler,
	)
}

//NewHandler will create Handler
func NewHandler(conf config.Config) http.Handler {
	transp := ConfigureHTTPTransport(conf)
	responseMerger := NewMultipleResponseHandler(conf)
	backends := make([]*url.URL, len(conf.Backends))
	for i, backend := range conf.Backends {
		backends[i] = backend.URL
	}
	httpTransport := transport.NewMultiTransport(
		transp,
		backends,
		responseMerger)

	roundTripper := DecorateRoundTripper(conf, httpTransport)
	return &Handler{
		config:       conf,
		mainLog:      conf.Mainlog,
		accessLog:    conf.Accesslog,
		roundTripper: roundTripper,
	}
}

//NewHandlerWithRoundTripper returns Handler, but will not construct transport.MultiTransport by itself
func NewHandlerWithRoundTripper(conf config.Config, roundTripper http.RoundTripper) http.Handler {
	return &Handler{
		config:       conf,
		mainLog:      conf.Mainlog,
		accessLog:    conf.Accesslog,
		roundTripper: roundTripper,
	}
}
