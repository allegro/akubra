package storages

import (
	"net/http"
)

// BasePicker contains common methods of pickers
type BasePicker struct {
	responsesChan <-chan BackendResponse
	success       BackendResponse
	failure       BackendResponse
	errors        []BackendResponse
	sent          bool
}

func (orp *BasePicker) collectSuccessResponse(bresp BackendResponse) {
	if orp.hasSuccessfulResponse() {
		bresp.DiscardBody()
	} else {
		orp.success = bresp
	}
}

func (orp *BasePicker) collectFailureResponse(bresp BackendResponse) {
	if orp.hasFailureResponse() {
		bresp.DiscardBody()
	} else {
		orp.failure = bresp
	}
	orp.errors = append(orp.errors, bresp)
}

func (orp *BasePicker) hasSuccessfulResponse() bool {
	return orp.success != BackendResponse{}
}

func (orp *BasePicker) hasFailureResponse() bool {
	return orp.failure != BackendResponse{}
}

func (orp *BasePicker) send(out chan<- BackendResponse, bresp BackendResponse) {
	out <- bresp
	orp.sent = true
}

// SendSyncLog implements picker interface
func (orp *BasePicker) SendSyncLog(*SyncSender) {}

// ObjectResponsePicker chooses first successful or one of failure response from chan of
// `BackendResponse`s
type ObjectResponsePicker struct {
	BasePicker
	syncLogReady chan struct{}
}

func newObjectResponsePicker(rch <-chan BackendResponse) picker {
	ch := make(chan struct{})
	return &ObjectResponsePicker{BasePicker: BasePicker{responsesChan: rch}, syncLogReady: ch}
}

// Pick returns first successful response, discard others
func (orp *ObjectResponsePicker) Pick() (*http.Response, error) {
	outChan := make(chan BackendResponse)
	go orp.pullResponses(outChan)
	bresp := <-outChan
	return bresp.Response, bresp.Error
}

// SendSyncLog implements picker interface
func (orp *ObjectResponsePicker) SendSyncLog(syncLog *SyncSender) {
	<-orp.syncLogReady
	sendSynclogs(syncLog, orp.success, orp.errors)

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
		if shouldSend {
			orp.send(out, bresp)
		}
	}

	if !orp.hasSuccessfulResponse() {
		out <- orp.failure
	}
	close(out)
	orp.syncLogReady <- struct{}{}
}

type deleteResponsePicker struct {
	BasePicker
}

// Pick returns first successful response, discard others
func (orp *deleteResponsePicker) Pick() (*http.Response, error) {
	outChan := make(chan BackendResponse)
	go orp.pullResponses(outChan)
	bresp := <-outChan
	return bresp.Response, bresp.Error
}

func newDeleteResponsePicker(rch <-chan BackendResponse) picker {
	return &deleteResponsePicker{BasePicker{responsesChan: rch}}
}

func (orp *deleteResponsePicker) pullResponses(out chan<- BackendResponse) {
	shouldSend := false
	for bresp := range orp.responsesChan {
		success := bresp.IsSuccessful()
		if success {
			orp.collectSuccessResponse(bresp)
		} else {
			shouldSend = !orp.hasFailureResponse()
			orp.collectFailureResponse(bresp)
		}
		if shouldSend {
			orp.send(out, bresp)
		}
	}

	if !orp.hasFailureResponse() {
		out <- orp.success
	}
	close(out)
}
