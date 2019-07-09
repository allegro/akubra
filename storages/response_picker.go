package storages

import (
	"net/http"

	"github.com/allegro/akubra/internal/akubra/log"
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

// ObjectResponsePicker chooses first successful or one of failure response from chan of
// `BackendResponse`s
type ObjectResponsePicker struct {
	BasePicker
}

func newFirstSuccessfulResponsePicker(rch <-chan BackendResponse) responsePicker {
	return &ObjectResponsePicker{BasePicker: BasePicker{responsesChan: rch}}
}

// Pick returns first successful response, discards others
func (orp *ObjectResponsePicker) Pick() (*http.Response, error) {
	outChan := make(chan BackendResponse)
	go orp.pullResponses(outChan)
	bresp := <-outChan
	return bresp.Response, bresp.Error
}

func (orp *ObjectResponsePicker) pullResponses(out chan<- BackendResponse) {
	shouldSend := false
	defer close(out)
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
}
type baseDeleteResponsePicker struct {
	BasePicker
	softErrors []BackendResponse
	respPuller func(drp *baseDeleteResponsePicker, out chan<- BackendResponse)
}

// Pick returns first successful response, discard others
func (drp *baseDeleteResponsePicker) Pick() (*http.Response, error) {
	outChan := make(chan BackendResponse)
	go drp.respPuller(drp, outChan)
	bresp := <-outChan
	return bresp.Response, bresp.Error
}

func newAllResponsesSuccessfulPicker(rch <-chan BackendResponse) responsePicker {
	return &baseDeleteResponsePicker{BasePicker{responsesChan: rch}, []BackendResponse{}, pullResponses}
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
}
