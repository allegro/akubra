package transport

import (
	"bufio"
	"context"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"
	"time"
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

// Create io.Writer and num []io.ReadCloser where all writer writes will be
// accessible by readers
func multiplicateReadClosers(num int) (writer io.Writer, readers []io.ReadCloser) {
	readers = make([]io.ReadCloser, 0, num)
	writers := make([]io.Writer, 0, num)
	for i := 0; i < num; i++ {
		pr, pw := io.Pipe()
		readers = append(readers, pr)
		writers = append(writers, pw)
	}
	writer = io.MultiWriter(writers...)
	return writer, readers
}

//MultipleResponsesHandler should handle chan of incomming ReqResErrTuple
//returned value's response and error will be passed to client
type MultipleResponsesHandler func(in <-chan *ReqResErrTuple) *ReqResErrTuple

func defaultHandleResponses(in <-chan *ReqResErrTuple, out chan<- *ReqResErrTuple) {
	errs := []*ReqResErrTuple{}
	clearBody := []*ReqResErrTuple{}
	respPassed := false
	for {
		r, ok := <-in
		if !ok {
			break
		}
		//discard body of successful responses if response already passed to client
		if respPassed {
			clearBody = append(clearBody, r)
		}
		//pass first successful answer to client
		if r.Err == nil && !respPassed {
			out <- r
			respPassed = true
		}
		//if error occured then append it into errs slice
		if r.Err != nil {
			if !respPassed {
				errs = append(errs, r)
			} else {
				//we passed response so discard response bodies as soon as possible
				clearBody = append(clearBody, r)
			}
		}
	}

	//if no response passed and has errors pass first one to client
	if !respPassed && len(errs) > 0 {
		out <- errs[0]
		// pop first error
		errs = errs[1:]
	}
	//close other error responses
	clearResponsesBody(append(errs, clearBody...))

}

func clearResponsesBody(respTups []*ReqResErrTuple) {
	for _, rtup := range respTups {
		if rtup.Res != nil {
			_, err := io.Copy(ioutil.Discard, rtup.Res.Body)
			if err != nil {
				rtup.Err = err
			}
		}
	}
}

//DefaultHandleResponses is default way of handling multiple responses.
//It will pass first success response or any error if no
//success occured
func DefaultHandleResponses(in <-chan *ReqResErrTuple) *ReqResErrTuple {
	out := make(chan *ReqResErrTuple, 1)
	go defaultHandleResponses(in, out)
	return <-out
}

//ErrTimeout is returned if TimeoutReader exceeds timeout
var ErrTimeout = errors.New("Read timeout")

//ErrBodyContentLengthMismatch is returned if request body is shorter than
//declared ContentLength header
var ErrBodyContentLengthMismatch = errors.New("Body ContentLength miss match")

//TimeoutReader returns error if cannot read any byte for Timeout duration
type TimeoutReader struct {
	//R is original reader
	R io.Reader
	//Timeout defines how long TimeoutReader will wait for next byte
	Timeout time.Duration
}

//Read implements io.Reader interface
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

//RequestProcessor helps change requests before roundtrip to backends
//orig is request received from client, copies will be send further
type RequestProcessor func(orig *http.Request, copies []*http.Request)

//MultiTransport replicates request onto multiple backends
type MultiTransport struct {
	http.RoundTripper
	//Backends is list of target endpoints URL
	Backends []*url.URL
	//Response handler will get `ReqResErrTuple` in `in` channel
	//should process all responses and send one to out chan.
	//Response senf to out chan will be returned from RoundTrip.
	//Remember to discard respose bodies if not read, otherwise
	//Keep-Alives won't function properly
	//
	//If `HandleResponses` is nil will pass first successful
	//(with status >= 200 & < 300) response or last failed.
	HandleResponses MultipleResponsesHandler
	//Process request between replication and sending, useful for changing request headers
	PreProcessRequest RequestProcessor
}

//ReplicateRequests creates request copies (one per MultiTransport.Bakcends item).
//New requests will have substituted Host field, original request body will be copied
//simultaneously
func (mt *MultiTransport) ReplicateRequests(req *http.Request, cancelFun context.CancelFunc) (reqs []*http.Request, err error) {
	copiesCount := len(mt.Backends)
	reqs = make([]*http.Request, 0, copiesCount)
	// We need some read closers
	writer, readers := multiplicateReadClosers(copiesCount)

	for i, reader := range readers {
		req.URL.Host = mt.Backends[i].Host
		body := io.LimitReader(reader, req.ContentLength)
		r, rerr := http.NewRequest(req.Method, req.URL.String(), body)
		// Copy request data
		if rerr != nil {
			return nil, rerr
		}
		r.Header = make(http.Header, len(req.Header))
		for k, v := range req.Header {
			r.Header[k] = make([]string, len(v))
			copy(r.Header[k], v)
		}
		r.ContentLength = req.ContentLength
		r.TransferEncoding = req.TransferEncoding
		reqs = append(reqs, r)
	}
	go func() {
		//Copy original request body to replicated requests bodies
		if req.Body != nil {
			bodyReader := &TimeoutReader{
				io.LimitReader(req.Body, req.ContentLength),
				time.Second}
			n, cerr := io.Copy(bufio.NewWriterSize(writer, int(req.ContentLength)), bodyReader)
			if cerr != nil || n < req.ContentLength {
				cancelFun()
			}
		}
	}()

	return reqs, err
}

func (mt *MultiTransport) sendRequest(
	req *http.Request,
	out chan *ReqResErrTuple,
	wg *sync.WaitGroup) {
	ctx := req.Context()
	o := make(chan *ReqResErrTuple)
	go func() {
		resp, err := mt.RoundTripper.RoundTrip(req)
		//report Non 2XX status codes as errors
		failed := err != nil || resp != nil && (resp.StatusCode < 200 || resp.StatusCode > 399)
		r := &ReqResErrTuple{req, resp, err, failed}
		o <- r
	}()
	var reqresperr *ReqResErrTuple
	select {
	case <-ctx.Done():
		reqresperr = &ReqResErrTuple{req, nil, ErrBodyContentLengthMismatch, true}
	case reqresperr = <-o:
		break
	}
	out <- reqresperr
}

//RoundTrip satisfies http.RoundTripper interface
func (mt *MultiTransport) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	bctx, cancelFunc := context.WithCancel(context.Background())

	reqs, err := mt.ReplicateRequests(req, cancelFunc)
	if err != nil {
		return nil, err
	}

	c := make(chan *ReqResErrTuple, len(reqs))
	if len(reqs) == 0 {
		return nil, errors.New("No requests provided")
	}

	wg := sync.WaitGroup{}
	for _, req := range reqs {
		wg.Add(1)
		r := req.WithContext(bctx)
		go func() {
			mt.sendRequest(r, c, &wg)
			wg.Done()
		}()
	}

	//close c chanel once all requests comes in
	go func() {
		wg.Wait()
		close(c)
	}()
	resTup := mt.HandleResponses(c)
	return resTup.Res, resTup.Err
}

//NewMultiTransport creates *MultiTransport. If requestsPreprocesor or responseHandler
//are nil will use default ones
func NewMultiTransport(roundTripper http.RoundTripper,
	backends []*url.URL,
	responsesHandler MultipleResponsesHandler) *MultiTransport {
	if responsesHandler == nil {
		responsesHandler = DefaultHandleResponses
	}
	if roundTripper == nil {
		roundTripper = http.DefaultTransport
	}

	return &MultiTransport{
		RoundTripper:    roundTripper,
		Backends:        backends,
		HandleResponses: responsesHandler}
}
