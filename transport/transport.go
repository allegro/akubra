package transport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	httphandlerConfig "github.com/allegro/akubra/httphandler/config"
	"github.com/allegro/akubra/log"
	"github.com/allegro/akubra/metrics"
	"github.com/allegro/akubra/transport/config"
)

const (
	defaultMaxIdleConnsPerHost = 100
)

// ResErrTuple is intermediate structure for internal use of
// HandleResponse function.
type ResErrTuple struct {
	// Received response
	Res *http.Response
	// First error occurred in transmision is passed here
	// Non 2XX response code is also treated as error
	Err    error
	Req    *http.Request
	Failed bool
	Time   time.Time
}

// DefinitionError properties for Transports
type DefinitionError struct {
	error
}

func discardReadCloser(rc io.ReadCloser) error {
	_, err := io.Copy(ioutil.Discard, rc)
	if err != nil {
		log.Printf("Discard body error %s", err)
		return err
	}
	err = rc.Close()
	if err != nil {
		log.Printf("Close body error %s", err)
	}
	return err
}

// DiscardBody clears request and response body
func (r *ResErrTuple) DiscardBody() error {
	ctx := r.Req.Context()
	requestID := ctx.Value(log.ContextreqIDKey)
	defer log.Printf("Linger duration %f %s", float64(time.Since(r.Time).Nanoseconds())/float64(1000000), requestID)
	defer metrics.UpdateSince("reqs.backend."+metrics.Clean(r.Req.URL.Host)+".linger", r.Time)
	if r.Req != nil && r.Req.Body != nil {
		if err := discardReadCloser(r.Req.Body); err != nil {
			log.Printf("Cannot discard request body: %s", err)
			return err
		}
	}

	if r.Res != nil && r.Res.Body != nil {
		log.Debugf("discard %s response body %s", r.Res.Request.URL.Host, requestID)
		if err := discardReadCloser(r.Res.Body); err != nil {
			log.Printf("Cannot discard request body: %s", err)
			return err
		}
	}
	return nil
}

// MultipleResponsesHandler should handle chan of incomming ReqResErrTuple
// returned value's response and error will be passed to client
type MultipleResponsesHandler func(in <-chan ResErrTuple) ResErrTuple

// Matcher mapping initialized Transports with http.RoundTripper by transport name
type Matcher struct {
	RoundTrippers    map[string]http.RoundTripper
	TransportsConfig config.Transports
}

func defaultHandleResponses(in <-chan ResErrTuple, out chan<- ResErrTuple) {
	errs := []ResErrTuple{}
	clearBody := []ResErrTuple{}
	respPassed := false
	for {
		r, ok := <-in
		if !ok {
			break
		}
		// discard body of successful responses if response already passed to client
		if respPassed {
			clearBody = append(clearBody, r)
		}
		// pass first successful answer to client
		if r.Err == nil && !respPassed {
			out <- r
			respPassed = true
		}
		// if error occurred then append it into errs slice
		if r.Err != nil {
			if !respPassed {
				errs = append(errs, r)
			} else {
				// we passed response so discard response bodies as soon as possible
				clearBody = append(clearBody, r)
			}
		}
	}

	// if no response passed and has errors pass first one to client
	if !respPassed && len(errs) > 0 {
		out <- errs[0]
		// pop first error
		errs = errs[1:]
	}
	// close other error responses
	clearResponsesBody(append(errs, clearBody...))

}

func clearResponsesBody(respTups []ResErrTuple) {
	for _, rtup := range respTups {
		if err := rtup.DiscardBody(); err != nil {
			log.Printf("ReqRespTup discard body error %s", err)
		}
	}
}

// DefaultHandleResponses is default way of handling multiple responses.
// It will pass first success response or any error if no
// success occurred
func DefaultHandleResponses(in <-chan ResErrTuple) ResErrTuple {
	out := make(chan ResErrTuple, 1)
	go defaultHandleResponses(in, out)
	return <-out
}

// ErrTimeout is returned if TimeoutReader exceeds timeout
var ErrTimeout = errors.New("Read timeout")

// ErrBodyContentLengthMismatch is returned if request body is shorter than
// declared ContentLength header
var ErrBodyContentLengthMismatch = errors.New("Body ContentLength miss match")

// TimeoutReader returns error if cannot read any byte for Timeout duration
type TimeoutReader struct {
	// R is original reader
	R io.Reader
	// Timeout defines how long TimeoutReader will wait for next byte
	Timeout time.Duration
}

// Read implements io.Reader interface
func (tr *TimeoutReader) Read(b []byte) (n int, err error) {
	gotsome := make(chan bool)
	go func() {
		n, err = tr.R.Read(b)
		gotsome <- true
	}()

	select {
	case <-time.After(tr.Timeout):
		return 0, ErrTimeout
	case <-gotsome:
		return
	}
}

// RequestProcessor helps change requests before roundtrip to backends
// orig is request received from client, copies will be send further
type RequestProcessor func(orig *http.Request, copies []*http.Request)

// MultiTransport replicates request onto multiple backends
type MultiTransport struct {
	// Backends is list of target endpoints URL
	Backends []http.RoundTripper
	// Response handler will get `ReqResErrTuple` in `in` channel
	// should process all responses and send one to out chan.
	// Response send to out chan will be returned from RoundTrip.
	// Remember to discard response bodies if not read, otherwise
	// Keep-Alives won't function properly
	//
	// If `HandleResponses` is nil will pass first successful
	// (with status >= 200 & < 300) response or last failed.
	HandleResponses MultipleResponsesHandler
	// Process request between replication and sending, useful for changing request headers
	PreProcessRequest RequestProcessor
}

// copyRequest creates request copies (one per MultiTransport.Bakcends item).
// New requests will have substituted Host field, original request body will be copied
// simultaneously
func (mt *MultiTransport) copyRequest(req *http.Request, cancelFun context.CancelFunc) (reqs []*http.Request, err error) {
	copiesCount := len(mt.Backends)
	reqs = make([]*http.Request, 0, copiesCount)
	// We need some read closers
	bodyBuffer := &bytes.Buffer{}
	bodyReader := &TimeoutReader{
		io.LimitReader(req.Body, req.ContentLength),
		time.Second}

	n, cerr := io.Copy(bodyBuffer, bodyReader)

	if cerr != nil || n < req.ContentLength {
		cancelFun()
		return nil, cerr
	}

	for range mt.Backends {
		log.Debugf("Replicate request %s", req.Context().Value(log.ContextreqIDKey))
		bodyContent := bodyBuffer.Bytes()
		var newBody io.Reader
		if len(bodyContent) > 0 {
			newBody = ioutil.NopCloser(bytes.NewReader(bodyContent))
		}
		r, rerr := http.NewRequest(req.Method, req.URL.String(), newBody)
		r = r.WithContext(req.Context())
		// Copy request data
		if rerr != nil {
			return nil, rerr
		}
		r.Header = make(http.Header, len(req.Header))
		for k, v := range req.Header {
			r.Header[k] = make([]string, len(v))
			copy(r.Header[k], v)
		}
		r.ContentLength = int64(bodyBuffer.Len())
		r.TransferEncoding = req.TransferEncoding
		r.Host = req.Host
		reqs = append(reqs, r)
		log.Debugf("Replicated request %s", r.Context().Value(log.ContextreqIDKey))
	}
	return reqs, err
}

func (mt *MultiTransport) sendRequest(
	req *http.Request,
	out chan ResErrTuple, backend http.RoundTripper) {
	ctx := req.Context()
	requestID := ctx.Value(log.ContextreqIDKey)

	output := make(chan ResErrTuple)

	go func() {
		resp, err := backend.RoundTrip(req.WithContext(context.WithValue(context.Background(), log.ContextreqIDKey, requestID)))
		if err != nil {
			log.Debugf("Send request error %s, %s", err.Error(), requestID)
		}
		log.Debugf("Sent request %s to %s", requestID, req.URL.Host)
		failed := err != nil || resp != nil && (resp.StatusCode < 200 || resp.StatusCode > 399)
		r := ResErrTuple{Res: resp, Err: err, Failed: failed, Time: time.Now()}
		output <- r
	}()
	var reqresperr ResErrTuple

	select {
	case <-ctx.Done():
		log.Debugf("Ctx Done reqID %s ", requestID)
		reqresperr = ResErrTuple{Res: nil, Err: ErrBodyContentLengthMismatch, Failed: true, Time: time.Now()}
	case reqresperr = <-output:
		break
	}
	reqresperr.Req = req
	out <- reqresperr
}

// RoundTrip satisfies http.RoundTripper interface
func (mt *MultiTransport) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	bctx, cancelFunc := context.WithCancel(context.Background())
	bctx = context.WithValue(bctx, log.ContextreqIDKey, req.Context().Value(log.ContextreqIDKey))
	reqs, err := mt.copyRequest(req, cancelFunc)

	if err != nil {
		return nil, err
	}

	responseTuplesChan := make(chan ResErrTuple, len(reqs))
	if len(reqs) == 0 {
		return nil, errors.New("No requests provided")
	}

	wg := sync.WaitGroup{}
	for i, backend := range mt.Backends {
		wg.Add(1)
		r := reqs[i].WithContext(bctx)
		log.Debugf("RoundTrip with ctxID %s\n", bctx.Value(log.ContextreqIDKey))
		go func(backend http.RoundTripper, request *http.Request) {
			mt.sendRequest(request, responseTuplesChan, backend)
			wg.Done()
		}(backend, r)
	}

	// close c chanel once all requests comes in
	go func() {
		wg.Wait()
		close(responseTuplesChan)
	}()
	resTup := mt.HandleResponses(responseTuplesChan)
	return resTup.Res, resTup.Err
}

// NewMultiTransport creates *MultiTransport. If requestsPreprocesor or responseHandler
// are nil will use default ones
func NewMultiTransport(backends []http.RoundTripper,
	responsesHandler MultipleResponsesHandler) *MultiTransport {
	if responsesHandler == nil {
		responsesHandler = DefaultHandleResponses
	}

	return &MultiTransport{
		Backends:        backends,
		HandleResponses: responsesHandler}
}

// SelectTransportDefinition returns transport instance by method, path and queryParams
func (m *Matcher) SelectTransportDefinition(method, path, queryParams string, log log.Logger) (matchedTransport config.TransportMatcherDefinition, err error) {
	matchedTransport, ok := m.TransportsConfig.GetMatchedTransportDefinition(method, path, queryParams)
	if !ok {
		errMsg := fmt.Sprintf("Transport not matched with args. method: %s, path: %s, queryParams: %s", method, path, queryParams)
		err = &DefinitionError{errors.New(errMsg)}
		log.Print(errMsg)
	}
	return
}

// RoundTrip for transport matching
func (m *Matcher) RoundTrip(request *http.Request) (response *http.Response, err error) {
	selectedRoundTriper, err := m.SelectTransportRoundTripper(request)
	if err == nil {
		return selectedRoundTriper.RoundTrip(request)
	}
	return
}

// SelectTransportRoundTripper for selecting RoundTripper by request object from transports matcher
func (m *Matcher) SelectTransportRoundTripper(request *http.Request) (selectedRoundTripper http.RoundTripper, err error) {
	selectedTransport, err := m.SelectTransportDefinition(request.Method, request.URL.Path, request.URL.RawQuery, log.DefaultLogger)
	if len(selectedTransport.Name) > 0 {
		reqID := request.Context().Value(log.ContextreqIDKey)
		log.Debugf("Request %s - selected transport name: %s (by method: %s, path: %s, queryParams: %s)",
			reqID, selectedTransport.Name, request.Method, request.URL.Path, request.URL.RawQuery)
		selectedRoundTripper = m.RoundTrippers[selectedTransport.Name]
	}

	return
}

// ConfigureHTTPTransports returns RoundTrippers mapped by transport name from configuration
func ConfigureHTTPTransports(clientConf httphandlerConfig.Client) (http.RoundTripper, error) {
	roundTrippers := make(map[string]http.RoundTripper)
	transportMatcher := &Matcher{TransportsConfig: clientConf.Transports}
	maxIdleConnsPerHost := defaultMaxIdleConnsPerHost
	if len(clientConf.Transports) > 0 {
		for _, transport := range clientConf.Transports {
			roundTrippers[transport.Name] = perepareTransport(transport.Properties, maxIdleConnsPerHost)
		}
		transportMatcher.RoundTrippers = roundTrippers
	} else {
		return nil, errors.New("Service->Server->Client->Transports config is empty")
	}

	return transportMatcher, nil
}

// perepareTransport with properties
func perepareTransport(properties config.ClientTransportProperties, maxIdleConnsPerHost int) http.RoundTripper {
	if properties.MaxIdleConnsPerHost != 0 {
		maxIdleConnsPerHost = properties.MaxIdleConnsPerHost
	}
	httpTransport := &http.Transport{
		MaxIdleConns:          properties.MaxIdleConns,
		MaxIdleConnsPerHost:   maxIdleConnsPerHost,
		IdleConnTimeout:       properties.IdleConnTimeout.Duration,
		ResponseHeaderTimeout: properties.ResponseHeaderTimeout.Duration,
		DisableKeepAlives:     properties.DisableKeepAlives,
	}
	return httpTransport
}
