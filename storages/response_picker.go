package storages

import (
	"net/http"
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
		bresp.DiscardBody()
	} else {
		bp.success = bresp
	}
}

func (bp *BasePicker) collectFailureResponse(bresp BackendResponse) {
	if bp.hasFailureResponse() || bp.sent {
		bresp.DiscardBody()
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
			bp.failure.DiscardBody()
		}
	} else if bp.hasSuccessfulResponse() {
		bp.success.DiscardBody()
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

type deleteResponsePicker struct {
	BasePicker
	softErrors   []BackendResponse
	syncLogReady chan struct{}
}

// Pick returns first successful response, discard others
func (drp *deleteResponsePicker) Pick() (*http.Response, error) {
	outChan := make(chan BackendResponse)
	go drp.pullResponses(outChan)
	bresp := <-outChan
	return bresp.Response, bresp.Error
}

func newDeleteResponsePicker(rch <-chan BackendResponse) responsePicker {
	return &deleteResponsePicker{BasePicker{responsesChan: rch}, []BackendResponse{}, make(chan struct{})}
}
func (drp *deleteResponsePicker) collectFailureResponse(bresp BackendResponse) {
	if bresp.Backend.Maintenance {
		drp.softErrors = append(drp.softErrors, bresp)
		return
	}
	drp.BasePicker.collectFailureResponse(bresp)
}

func (drp *deleteResponsePicker) pullResponses(out chan<- BackendResponse) {
	shouldSend := false
	for bresp := range drp.responsesChan {
		success := bresp.IsSuccessful()
		if success {
			drp.collectSuccessResponse(bresp)
		} else {
			shouldSend = !drp.hasFailureResponse() && bresp.Backend.Maintenance == false
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

// SendSyncLog implements picker interface
func (drp *deleteResponsePicker) SendSyncLog(syncLog *SyncSender) {
	<-drp.syncLogReady
	sendSynclogs(syncLog, drp.success, drp.softErrors)
}
