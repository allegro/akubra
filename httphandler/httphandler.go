package httphandler

import (
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

//NewHandler will create Handler
func NewHandler(conf config.Config) http.Handler {
	mainlog := conf.Mainlog
	rh := &responseMerger{
		conf.Synclog,
		mainlog,
		conf.SyncLogMethodsSet}

	connDuration, _ := time.ParseDuration(conf.ConnectionTimeout)
	dialDuration, _ := time.ParseDuration(conf.ConnectionTimeout)
	var dialer *dial.LimitDialer

	dialer = dial.NewLimitDialer(conf.ConnLimit, connDuration, dialDuration)
	if len(conf.MaintainedBackend) > 0 {
		dialer.DropEndpoint(conf.MaintainedBackend)
	}

	httpTransport := &http.Transport{
		Dial:                dialer.Dial,
		DisableKeepAlives:   conf.KeepAlive,
		MaxIdleConnsPerHost: int(conf.ConnLimit)}
	backends := make([]*url.URL, len(conf.Backends))
	for _, backend := range(conf.Backends) {
		backends = append(backends, backend.URL)
	}
	multiTransport := transport.NewMultiTransport(
		httpTransport,
		backends,
		rh.handleResponses)
	roundTripper := Decorate(
		multiTransport,
		HeadersSuplier(conf.AdditionalRequestHeaders, conf.AdditionalResponseHeaders),
		AccessLogging(conf.Accesslog),
	)
	return &Handler{
		config:       conf,
		mainLog:      mainlog,
		accessLog:    conf.Accesslog,
		roundTripper: roundTripper,
	}
}
