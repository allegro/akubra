package storages

// BucketResponsePicker chooses first successful or one of failure response from chan of
// `BackendResponse`s
type BucketResponsePicker struct {
	responsesChan <-chan BackendResponse
	success       BackendResponse
	failure       BackendResponse
	errors        []BackendResponse
}

// func newBucketResponsePicker(rch <-chan BackendResponse) picker {
// 	return &BucketResponsePicker{responsesChan: rch}
// }

// Pick returns first successful response, discard others
// func (orp *BucketResponsePicker) Pick() (*http.Response, error) {
// 	outChan := make(chan BackendResponse)
// 	go orp.pullResponses(outChan)
// 	bresp := <-outChan
// 	return bresp.Response, bresp.Error
// }

// func (orp *BucketResponsePicker) pullResponses(out chan<- BackendResponse) {
// 	for bresp := range orp.responsesChan {
// 		success := isSuccessfullBackendResponse(bresp)
// 		if success {
// 			orp.collectSuccessResponse(out, bresp)
// 		} else {
// 			orp.collectFailureResponse(bresp)
// 		}
// 	}

// 	if !orp.hasSuccessfulResponse() {
// 		out <- orp.failure
// 	}
// 	close(out)
// }

// func (orp *BucketResponsePicker) collectSuccessResponse(out chan<- BackendResponse, bresp BackendResponse) {
// 	if orp.hasSuccessfulResponse() {
// 		bresp.DiscardBody()
// 	} else {
// 		orp.success = bresp
// 		out <- bresp
// 	}
// 	if orp.hasFailureResponse() {
// 		orp.failure.DiscardBody()
// 	}
// }

// func (orp *BucketResponsePicker) collectFailureResponse(bresp BackendResponse) {
// 	log.Print("Process failure")
// 	if orp.hasFailureResponse() {
// 		log.Print("Already has failure")
// 		bresp.DiscardBody()
// 	} else {
// 		log.Print("Memorize failure", bresp)
// 		orp.failure = bresp
// 	}
// 	orp.errors = append(orp.errors, bresp)
// }

// func (orp *BucketResponsePicker) hasSuccessfulResponse() bool {
// 	return orp.success != BackendResponse{}
// }

// func (orp *BucketResponsePicker) hasFailureResponse() bool {
// 	log.Println(orp.failure, BackendResponse{}, orp.failure != BackendResponse{})
// 	return orp.failure != BackendResponse{}
// }

// func isSuccessfullBackendResponse(bresp BackendResponse) bool {
// 	return bresp.Error == nil && bresp.Response != nil && bresp.Response.StatusCode >= 200 && bresp.Response.StatusCode < 400
// }
