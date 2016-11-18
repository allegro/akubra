package httphandler

import (
	"io"
	"io/ioutil"
	"log"

	"github.com/allegro/akubra/transport"
	set "github.com/deckarep/golang-set"
)

type responseMerger struct {
	syncerrlog      *log.Logger
	runtimeLog      *log.Logger
	methodSetFilter set.Set
}

func (rd *responseMerger) synclog(r, successfulTup *transport.ReqResErrTuple) {
	//don't log if request method was not included in configuration
	if rd.methodSetFilter == nil || !rd.methodSetFilter.Contains(r.Req.Method) {
		return
	}
	//do not log if backend response was successful
	if !r.Failed {
		return
	}
	//do not log if there was no successful response
	if successfulTup == nil {
		return
	}
	//log error entry
	errorMsg := "No error"
	if r.Err != nil {
		errorMsg = r.Err.Error()
	}
	syncLogMsg := NewSyncLogMessageData(
		r.Req.Method,
		r.Req.Host,
		successfulTup.Req.URL.Path,
		successfulTup.Req.Host,
		r.Req.Header.Get("User-Agent"),
		errorMsg)
	logMsg, err := syncLogMsg.JSON()
	if err != nil {
		return
	}
	rd.syncerrlog.Println(string(logMsg))
}

func (rd *responseMerger) handleFailedResponces(
	tups []*transport.ReqResErrTuple,
	out chan<- *transport.ReqResErrTuple,
	alreadysent bool,
	successfulTup *transport.ReqResErrTuple,
	logMethodSet set.Set) bool {

	for _, r := range tups {
		errorMsg := "No error"
		if r.Err != nil {
			errorMsg = r.Err.Error()
		}

		rd.runtimeLog.Printf("RGW resp %q, %q, %q, %t, %q",
			r.Req.URL.Path,
			r.Req.Method,
			r.Req.Host,
			r.Failed,
			errorMsg)

		rd.synclog(r, successfulTup)

		if !alreadysent {
			out <- r
			alreadysent = true
			continue //don't discard body
		}
		//discard body
		if r.Res != nil && r.Res.Body != nil {
			_, err := io.Copy(ioutil.Discard, r.Res.Body)
			if err != nil {
				rd.runtimeLog.Printf("Could not discard body %s", err)
			}
		}
	}

	return alreadysent
}

func (rd *responseMerger) _handle(in <-chan *transport.ReqResErrTuple, out chan<- *transport.ReqResErrTuple) {
	var successfulTup *transport.ReqResErrTuple
	errs := []*transport.ReqResErrTuple{}
	nonErrs := []*transport.ReqResErrTuple{}
	respPassed := false

	for {
		r, hasMore := <-in
		if !hasMore {
			break
		}
		//pass first successful answer to client
		if !r.Failed && !respPassed {
			//append additional headers
			successfulTup = r
			out <- r
			respPassed = true
			continue
		}
		if r.Err != nil {
			errs = append(errs, r)
		} else {
			nonErrs = append(nonErrs, r)
		}
	}

	respPassed = rd.handleFailedResponces(nonErrs, out, respPassed, successfulTup, rd.methodSetFilter)
	rd.handleFailedResponces(errs, out, respPassed, successfulTup, rd.methodSetFilter)
}

func (rd *responseMerger) handleResponses(in <-chan *transport.ReqResErrTuple) *transport.ReqResErrTuple {
	out := make(chan *transport.ReqResErrTuple, 1)
	go func() {
		rd._handle(in, out)
		close(out)
	}()
	return <-out
}
