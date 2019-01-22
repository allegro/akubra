package storages

import (
	"net/http"

	"github.com/allegro/akubra/log"
	"github.com/allegro/akubra/watchdog"
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
	consistencyRecord   *watchdog.ConsistencyRecord
	consistencyWatchdog watchdog.ConsistencyWatchdog
}

func newFirstSuccessfulResponsePicker(rch <-chan BackendResponse,
	watchdog watchdog.ConsistencyWatchdog,
	consistencyRecord *watchdog.ConsistencyRecord) responsePicker {
	return &ObjectResponsePicker{BasePicker: BasePicker{responsesChan: rch}, consistencyWatchdog: watchdog,
		consistencyRecord: consistencyRecord}
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
		return
	}

	if !orp.hasFailureResponse() || orp.consistencyRecord == nil {
		return
	}

	orp.performReadRepair()
}

func (orp *ObjectResponsePicker) performReadRepair() {
	objectVersion := orp.
		success.
		Request.
		Header.
		Get(orp.consistencyWatchdog.GetVersionHeaderName())

	if objectVersion == "" {
		return
	}

	orp.consistencyRecord.ObjectVersion = objectVersion
	_, err := orp.consistencyWatchdog.Insert(orp.consistencyRecord)
	if err != nil {
		log.Debugf("Failed to perform read repair for object %s in domain %s: %s",
			orp.consistencyRecord.ObjectID, orp.consistencyRecord.Domain, err)
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

func newAllResponsesSuccessfulPicker(rch <-chan BackendResponse, _ watchdog.ConsistencyWatchdog, _ *watchdog.ConsistencyRecord) responsePicker {
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
