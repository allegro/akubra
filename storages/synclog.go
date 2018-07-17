package storages

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/allegro/akubra/httphandler"
	"github.com/allegro/akubra/log"
	"github.com/allegro/akubra/metrics"
	"github.com/allegro/akubra/types"
	"github.com/allegro/akubra/utils"
)

// SyncSender filters and writes inconsistencies to synclog
type SyncSender struct {
	AllowedMethods map[string]struct{}
	SyncLog        log.Logger
}

func (slf SyncSender) shouldResponseBeLogged(bresp BackendResponse) bool {
	if slf.AllowedMethods == nil {
		return false
	}
	_, allowed := slf.AllowedMethods[bresp.Request.Method]
	if slf.SyncLog == nil || !allowed {
		return false
	}

	if bresp.Error == ErrRequestCanceled {
		return false
	}
	return true
}

func (slf SyncSender) send(success, failure BackendResponse) {
	if shouldBeFilteredInMaintenanceMode(success, failure) {
		return
	}

	errorMsg := emptyStrOrErrorMsg(failure.Error)
	contentLength := success.Response.ContentLength
	reqID := utils.RequestID(success.Request)

	syncLogMsg := &httphandler.SyncLogMessageData{
		Method:        success.Request.Method,
		FailedHost:    extractDestinationHostName(failure),
		SuccessHost:   extractDestinationHostName(success),
		Path:          success.Request.URL.Path,
		AccessKey:     utils.ExtractAccessKey(success.Request),
		UserAgent:     success.Request.Header.Get("User-Agent"),
		ContentLength: contentLength,
		ErrorMsg:      errorMsg,
		ReqID:         reqID,
		Time:          time.Now().Format(time.RFC3339Nano),
	}

	metrics.Mark(fmt.Sprintf("reqs.inconsistencies.%s.method-%s", metrics.Clean(failure.Backend.Endpoint.Host), success.Request.Method))
	logMsg, err := json.Marshal(syncLogMsg)
	if err != nil {
		log.Debugf("Marshall synclog error %s", err)
		return
	}
	slf.SyncLog.Println(string(logMsg))
}

func sendSynclogs(syncLog *SyncSender, success BackendResponse, failures []BackendResponse) {
	if len(failures) == 0 || (success == BackendResponse{}) || syncLog == nil || !syncLog.shouldResponseBeLogged(failures[0]) {
		return
	}
	for _, failure := range failures {
		syncLog.send(success, failure)
	}

}

func emptyStrOrErrorMsg(err error) string {
	if err != nil {
		return fmt.Sprintf("non nil error:%s", err)
	}
	return ""
}

func shouldBeFilteredInMaintenanceMode(success, failure BackendResponse) bool {
	if !failure.Backend.Maintenance {
		return false
	}
	isPutOrDelMethod := (success.Request.Method == http.MethodPut) || (success.Request.Method == http.MethodDelete)
	return !isPutOrDelMethod
}

// extractDestinationHostName extract destination hostname from request
func extractDestinationHostName(r BackendResponse) string {
	if r.Backend != nil {
		return r.Backend.Endpoint.Host
	}
	if r.Request != nil {
		return r.Request.URL.Host
	}
	berr, ok := r.Error.(*types.BackendError)
	if ok {
		return berr.Backend()
	}
	log.Printf("Requested backend is not retrievable from tuple %#v", r)
	return ""
}
