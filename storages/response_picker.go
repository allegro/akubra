package storages

import (
	"net/http"

	"github.com/allegro/akubra/log"
)

var emptyBackendResponse = BackendResponse{}
// BasePicker contains common methods of pickers
type BasePicker struct {
	responsesChan <-chan BackendResponse
	success       BackendResponse
	failure       BackendResponse
	errors        []BackendResponse
	sent          bool
}

func (bp *BasePicker) collectSuccessResponse(bresp BackendResponse) {
	if bp.hasSuccessfulResponse() || bp.sent {
		if err := bresp.DiscardBody(); err != nil {
			log.Debugf("Could not close tuple body: %s", err)
		}
	} else {
		bp.success = bresp
	}
}

func (bp *BasePicker) collectFailureResponse(bresp BackendResponse) {
	if bp.hasFailureResponse() || bp.sent {
		if err := bresp.DiscardBody(); err != nil {
			log.Debugf("Could not close tuple body: %s", err)
		}
	} else {
		bp.failure = bresp
	}
	bp.errors = append(bp.errors, bresp)
}

func (bp *BasePicker) hasSuccessfulResponse() bool {
	return bp.success != emptyBackendResponse
}

func (bp *BasePicker) hasFailureResponse() bool {
	return bp.failure != emptyBackendResponse
}

func (bp *BasePicker) send(out chan<- BackendResponse, bresp BackendResponse) {
	out <- bresp
	bp.sent = true
	if bresp.IsSuccessful() {
		if bp.hasFailureResponse() {
			if err := bp.failure.DiscardBody(); err != nil {
				log.Debugf("Could not close tuple body: %s", err)
			}

		}
	} else if bp.hasSuccessfulResponse() {
		if err := bp.success.DiscardBody(); err != nil {
			log.Debugf("Could not close tuple body: %s", err)
		}
	}
}

// SendSyncLog implements picker interface
func (bp *BasePicker) SendSyncLog(*SyncSender) {}

// ObjectResponsePicker chooses first successful or one of failure response from chan of
// `BackendResponse`s
type ObjectResponsePicker struct {
	BasePicker
	syncLogReady chan struct{}
}

func newObjectResponsePicker(rch <-chan BackendResponse) responsePicker {
	ch := make(chan struct{})
	return &ObjectResponsePicker{BasePicker: BasePicker{responsesChan: rch}, syncLogReady: ch}
}

// Pick returns first successful response, discards others
func (orp *ObjectResponsePicker) Pick() (*http.Response, error) {
	outChan := make(chan BackendResponse)
	go orp.pullResponses(outChan)
	bresp := <-outChan
	return bresp.Response, bresp.Error
}

// SendSyncLog implements picker interface
func (orp *ObjectResponsePicker) SendSyncLog(syncLog *SyncSender) {
	for range orp.syncLogReady {
		sendSynclogs(syncLog, orp.success, orp.errors)
	}
}

func (orp *ObjectResponsePicker) pullResponses(out chan<- BackendResponse) {
	shouldSend := false
	for bresp := range orp.responsesChan {
		success := bresp.IsSuccessful()
		if success {
			shouldSend = !orp.hasSuccessfulResponse()
			orp.collectSuccessResponse(bresp)
		} else {
			orp.collectFailureResponse(bresp)
		}
		if shouldSend && bresp.IsSuccessful() {
			orp.send(out, bresp)
		}
	}

	if !orp.hasSuccessfulResponse() {
		orp.send(out, orp.failure)
	}
	close(out)
	orp.syncLogReady <- struct{}{}
	close(orp.syncLogReady)
}

type baseDeleteResponsePicker struct {
	BasePicker
	softErrors   []BackendResponse
	syncLogReady chan struct{}
	respPuller func(drp *baseDeleteResponsePicker, out chan<- BackendResponse)
}

// Pick returns first successful response, discard others
func (drp *baseDeleteResponsePicker) Pick() (*http.Response, error) {
	outChan := make(chan BackendResponse)
	go drp.respPuller(drp, outChan)
	bresp := <-outChan
	return bresp.Response, bresp.Error
}

func newDeleteResponsePicker(rch <-chan BackendResponse) responsePicker {
	return &baseDeleteResponsePicker{BasePicker{responsesChan: rch}, []BackendResponse{}, make(chan struct{}), pullResponses}
}

func newDeleteResponsePickerWatchdog(rch <-chan BackendResponse) responsePicker {
	return &baseDeleteResponsePicker{BasePicker{responsesChan: rch}, []BackendResponse{}, make(chan struct{}), pullResponsesWatchdog}
}

func (drp *baseDeleteResponsePicker) collectFailureResponse(bresp BackendResponse) {
	if bresp.Backend.Maintenance {
		drp.softErrors = append(drp.softErrors, bresp)
		return
	}
	drp.BasePicker.collectFailureResponse(bresp)
}

func pullResponses(drp *baseDeleteResponsePicker, out chan<- BackendResponse) {
	shouldSend := false
	for bresp := range drp.responsesChan {
		success := bresp.IsSuccessful()
		if success {
			drp.collectSuccessResponse(bresp)
		} else {
			shouldSend = !drp.hasFailureResponse() && !bresp.Backend.Maintenance
			drp.collectFailureResponse(bresp)
		}
		if shouldSend {
			drp.send(out, bresp)
		}
	}

	if !drp.hasFailureResponse() {
		drp.send(out, drp.success)
	}
	close(out)
	drp.syncLogReady <- struct{}{}
	close(drp.syncLogReady)
}


func pullResponsesWatchdog(drp *baseDeleteResponsePicker, out chan<- BackendResponse) {
	shouldSend := false
	for bresp := range drp.responsesChan {
		success := bresp.IsSuccessful()
		if success {
			shouldSend = true

		} else {
			drp.collectFailureResponse(bresp)
			if drp.failure == emptyBackendResponse {
				drp.failure = bresp
			}
		}
		if shouldSend && drp.success == emptyBackendResponse {
			drp.success = bresp
			drp.send(out, bresp)
		}
	}
	if drp.success == emptyBackendResponse {
		drp.send(out, drp.failure)
	}
	close(out)
	drp.syncLogReady <- struct{}{}
	close(drp.syncLogReady)
}

// SendSyncLog implements picker interface
func (drp *baseDeleteResponsePicker) SendSyncLog(syncLog *SyncSender) {
	<-drp.syncLogReady
	sendSynclogs(syncLog, drp.success, drp.softErrors)
}

