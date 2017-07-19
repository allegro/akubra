package transport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/allegro/akubra/log"
	"github.com/allegro/akubra/metrics"
	shardingconfig "github.com/allegro/akubra/sharding/config"
)

// ReqResErrTuple is intermediate structure for internal use of
// HandleResponse function.
type ReqResErrTuple struct {
	// Sent request
	Req *http.Request
	// Received response
	Res *http.Response
	// First error occured in transmision is passed here
	// Non 2XX response code is also treated as error
	Err    error
	Failed bool
}

// MultipleResponsesHandler should handle chan of incomming ReqResErrTuple
// returned value's response and error will be passed to client
type MultipleResponsesHandler func(in <-chan ReqResErrTuple) ReqResErrTuple

func defaultHandleResponses(in <-chan ReqResErrTuple, out chan<- ReqResErrTuple) {
	errs := []ReqResErrTuple{}
	clearBody := []ReqResErrTuple{}
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
		// if error occured then append it into errs slice
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

func clearResponsesBody(respTups []ReqResErrTuple) {
	for _, rtup := range respTups {
		if rtup.Res != nil {
			_, err := io.Copy(ioutil.Discard, rtup.Res.Body)
			if err != nil {
				rtup.Err = err
			}
		}
	}
}

// DefaultHandleResponses is default way of handling multiple responses.
// It will pass first success response or any error if no
// success occured
func DefaultHandleResponses(in <-chan ReqResErrTuple) ReqResErrTuple {
	out := make(chan ReqResErrTuple, 1)
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
	http.RoundTripper
	// Backends is list of target endpoints URL
	Backends     []url.URL
	SkipBackends map[string]bool
	// Response handler will get `ReqResErrTuple` in `in` channel
	// should process all responses and send one to out chan.
	// Response senf to out chan will be returned from RoundTrip.
	// Remember to discard respose bodies if not read, otherwise
	// Keep-Alives won't function properly
	//
	// If `HandleResponses` is nil will pass first successful
	// (with status >= 200 & < 300) response or last failed.
	HandleResponses MultipleResponsesHandler
	// Process request between replication and sending, useful for changing request headers
	PreProcessRequest RequestProcessor
}

// ReplicateRequests creates request copies (one per MultiTransport.Bakcends item).
// New requests will have substituted Host field, original request body will be copied
// simultaneously
func (mt *MultiTransport) ReplicateRequests(req *http.Request, cancelFun context.CancelFunc) (reqs []*http.Request, err error) {
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

	for _, backend := range mt.Backends {
		req.URL.Host = backend.Host
		log.Debugf("Replicate request %s, for %s", req.Context().Value(log.ContextreqIDKey), backend.Host)

		bodyContent := bodyBuffer.Bytes()
		var newBody io.Reader
		if len(bodyContent) > 0 {
			newBody = ioutil.NopCloser(bytes.NewReader(bodyContent))
		}
		r, rerr := http.NewRequest(req.Method, req.URL.String(), newBody)
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
		reqs = append(reqs, r)
	}

	return reqs, err
}

func collectMetrics(req *http.Request, reqresperr ReqResErrTuple, since time.Time) {
	host := metrics.Clean(req.URL.Host)
	metrics.UpdateSince("reqs.backend."+host+".all", since)
	if reqresperr.Err != nil {
		metrics.UpdateSince("reqs.backend."+host+".err", since)
	}
	if reqresperr.Res != nil {
		statusName := fmt.Sprintf("reqs.backend."+host+".status_%d", reqresperr.Res.StatusCode)
		metrics.UpdateSince(statusName, since)
	}
	if reqresperr.Req != nil {
		methodName := fmt.Sprintf("reqs.backend."+host+".method_%s", reqresperr.Req.Method)
		metrics.UpdateSince(methodName, since)
	}
}

func (mt *MultiTransport) sendRequest(
	req *http.Request,
	out chan ReqResErrTuple) {
	since := time.Now()
	ctx := req.Context()
	o := make(chan ReqResErrTuple)
	go func() {
		if mt.SkipBackends[req.URL.Host] {
			log.Debugf("Skipping request %s, for %s", req.Context().Value(log.ContextreqIDKey), req.URL.Host)
			r := ReqResErrTuple{req, nil, fmt.Errorf("Maintained Backend %s", req.URL.Host), true}
			o <- r
			return
		}

		resp, err := mt.RoundTripper.RoundTrip(req.WithContext(context.Background()))
		// report Non 2XX status codes as errors
		if err != nil {
			log.Debugf("Send request error %s, %s", err.Error(), ctx.Value(log.ContextreqIDKey))
		}
		failed := err != nil || resp != nil && (resp.StatusCode < 200 || resp.StatusCode > 399)
		r := ReqResErrTuple{req, resp, err, failed}
		o <- r
	}()
	var reqresperr ReqResErrTuple
	defer collectMetrics(req, reqresperr, since)

	select {
	case <-ctx.Done():
		log.Debugf("Ctx Done reqID %s ", ctx.Value(log.ContextreqIDKey))
		reqresperr = ReqResErrTuple{req, nil, ErrBodyContentLengthMismatch, true}
	case reqresperr = <-o:
		break
	}
	out <- reqresperr
}

// RoundTrip satisfies http.RoundTripper interface
func (mt *MultiTransport) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	bctx, cancelFunc := context.WithCancel(context.Background())
	bctx = context.WithValue(bctx, log.ContextreqIDKey, req.Context().Value(log.ContextreqIDKey))
	reqs, err := mt.ReplicateRequests(req, cancelFunc)
	if err != nil {
		return nil, err
	}

	c := make(chan ReqResErrTuple, len(reqs))
	if len(reqs) == 0 {
		return nil, errors.New("No requests provided")
	}

	wg := sync.WaitGroup{}
	for _, req := range reqs {
		wg.Add(1)
		r := req.WithContext(bctx)
		go func() {
			mt.sendRequest(r, c)
			wg.Done()
		}()
	}

	// close c chanel once all requests comes in
	go func() {
		wg.Wait()
		close(c)
	}()
	resTup := mt.HandleResponses(c)
	return resTup.Res, resTup.Err
}

// NewMultiTransport creates *MultiTransport. If requestsPreprocesor or responseHandler
// are nil will use default ones
func NewMultiTransport(roundTripper http.RoundTripper,
	backends []url.URL,
	responsesHandler MultipleResponsesHandler,
	maintainedBackends []shardingconfig.YAMLUrl) *MultiTransport {
	if responsesHandler == nil {
		responsesHandler = DefaultHandleResponses
	}
	if roundTripper == nil {
		roundTripper = http.DefaultTransport
	}
	mb := make(map[string]bool, len(maintainedBackends))
	for _, yurl := range maintainedBackends {
		mb[yurl.Host] = true
	}

	return &MultiTransport{
		RoundTripper:    roundTripper,
		Backends:        backends,
		SkipBackends:    mb,
		HandleResponses: responsesHandler}
}
