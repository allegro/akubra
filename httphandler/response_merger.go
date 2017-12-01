package httphandler

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/allegro/akubra/log"
	"github.com/allegro/akubra/metrics"
	"github.com/allegro/akubra/transport"
	set "github.com/deckarep/golang-set"
)

type responseMerger struct {
	syncerrlog      log.Logger
	methodSetFilter set.Set
	fifo            bool
}

func (rd *responseMerger) synclog(r, successfulTup transport.ResErrTuple) {
	// don't log if request method was not included in configuration
	if rd.methodSetFilter == nil || !rd.methodSetFilter.Contains(r.Req.Method) {
		return
	}
	// do not log if backend response was successful
	if !r.Failed {
		return
	}
	// do not log if there was no successful response
	if (successfulTup == transport.ResErrTuple{}) {
		return
	}
	// log error entry
	errorMsg := "No error"
	if r.Err != nil {
		errorMsg = r.Err.Error()
	}

	contentLength := successfulTup.Res.ContentLength
	reqID := "xxxxxxxxxxxxxxx"
	if r.Res != nil {
		reqID = r.Res.Request.Context().Value(log.ContextreqIDKey).(string)
	}

	syncLogMsg := NewSyncLogMessageData(
		r.Req.Method,
		r.Req.URL.Host,
		successfulTup.Req.URL.Path,
		successfulTup.Req.URL.Host,
		r.Req.Header.Get("User-Agent"),
		reqID,
		errorMsg,
		contentLength)
	metrics.Mark(fmt.Sprintf("reqs.inconsistencies.%s.method-%s", metrics.Clean(r.Req.Host), r.Req.Method))
	logMsg, err := json.Marshal(syncLogMsg)
	if err != nil {
		return
	}
	rd.syncerrlog.Println(string(logMsg))
}

func (rd *responseMerger) handleFailedResponces(
	tups []transport.ResErrTuple,
	out chan<- transport.ResErrTuple,
	alreadysent bool,
	successfulTup transport.ResErrTuple,
	logMethodSet set.Set) bool {

	for _, r := range tups {
		rd.synclog(r, successfulTup)

		if !alreadysent {
			out <- r
			alreadysent = true
			continue // don't discard body
		}
		// discard body
		if r.Res != nil && r.Res.Body != nil {
			_, err := io.Copy(ioutil.Discard, r.Res.Body)
			if err != nil {
				log.Printf("Could not discard body %s", err)
			}
			err = r.Res.Body.Close()
			if err != nil {
				log.Printf("Could not close body %s", err)
			}
		}
	}

	return alreadysent
}

func (rd *responseMerger) _handle(in <-chan transport.ResErrTuple, out chan<- transport.ResErrTuple) {
	var successfulTup transport.ResErrTuple
	errs := []transport.ResErrTuple{}
	nonErrs := []transport.ResErrTuple{}
	firstPassed := false

	for {
		r, hasMore := <-in
		if !hasMore {
			break
		}

		statusCode := 0
		reqID := "xxxxxxxxxxxxxxx"
		if r.Res != nil {
			statusCode = r.Res.StatusCode
			reqID, _ = r.Res.Request.Context().Value(log.ContextreqIDKey).(string)
		}
		log.Debugf("Got response %s from backend %s, status: %d, method: %s, path %s, error: %q",
			reqID,
			r.Req.URL.Host,
			statusCode,
			r.Req.Method,
			r.Req.URL.Path,
			r.Err)

		if !r.Failed && !firstPassed {
			successfulTup = r
			if rd.fifo {
				out <- r
			}
			firstPassed = true
			continue
		}
		if r.Err != nil {
			errs = append(errs, r)
		} else {
			nonErrs = append(nonErrs, r)
		}
	}

	if !rd.fifo && firstPassed {
		out <- successfulTup
	}

	firstPassed = rd.handleFailedResponces(nonErrs, out, firstPassed, successfulTup, rd.methodSetFilter)
	rd.handleFailedResponces(errs, out, firstPassed, successfulTup, rd.methodSetFilter)
}

func (rd *responseMerger) handleResponses(in <-chan transport.ResErrTuple) transport.ResErrTuple {
	out := make(chan transport.ResErrTuple, 1)
	go func() {
		rd._handle(in, out)
		close(out)
	}()
	return <-out
}

// EarliestResponseHandler returns a function which handles multiple
// responses, returns first successful response to caller
func EarliestResponseHandler(synclog log.Logger, methods set.Set) transport.MultipleResponsesHandler {
	rh := responseMerger{
		syncerrlog:      synclog,
		methodSetFilter: methods,
		fifo:            true,
	}
	return rh.handleResponses
}

// LateResponseHandler returns a function which handles multiple
// responses and returns first successful response to caller after
// all other responces received
func LateResponseHandler(synclog log.Logger, methods set.Set) transport.MultipleResponsesHandler {
	rh := responseMerger{
		syncerrlog:      synclog,
		methodSetFilter: methods,
		fifo:            false,
	}
	return rh.handleResponses
}
