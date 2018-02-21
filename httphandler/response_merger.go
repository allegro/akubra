package httphandler

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/allegro/akubra/log"
	"github.com/allegro/akubra/metrics"
	"github.com/allegro/akubra/transport"
	"github.com/allegro/akubra/utils"
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
	reqID := utils.RequestID(successfulTup.Req)

	syncLogMsg := &SyncLogMessageData{
		Method:        r.Req.Method,
		FailedHost:    utils.ExtractDestinationHostName(r),
		SuccessHost:   utils.ExtractDestinationHostName(successfulTup),
		Path:          successfulTup.Req.URL.Path,
		AccessKey:     utils.ExtractAccessKey(r.Req),
		UserAgent:     successfulTup.Req.Header.Get("User-Agent"),
		ContentLength: contentLength,
		ErrorMsg:      errorMsg,
		ReqID:         reqID,
		Time:          time.Now().Format(time.RFC3339Nano),
	}

	metrics.Mark(fmt.Sprintf("reqs.inconsistencies.%s.method-%s", metrics.Clean(r.Req.Host), r.Req.Method))
	logMsg, err := json.Marshal(syncLogMsg)
	if err != nil {
		log.Debugf("Marshall synclog error %s", err)
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
			r.DiscardBody()
		}
	}

	return alreadysent
}

func logDebug(r transport.ResErrTuple) {
	reqID := utils.RequestID(r.Req)
	backend := utils.ExtractDestinationHostName(r)

	statusCode := 0
	if r.Res != nil {
		statusCode = r.Res.StatusCode
	}

	log.Debugf("Got response %s from backend %s, status: %d, method: %s, path %s, error: %q",
		reqID,
		backend,
		statusCode,
		r.Req.Method,
		r.Req.URL.Path,
		r.Err)
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
		logDebug(r)
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
