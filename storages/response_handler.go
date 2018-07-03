package storages

import (
	"net/http"
	"strings"

	"github.com/allegro/akubra/log"
	"github.com/allegro/akubra/storages/merger"
)

const listTypeV2 = "2"

type responseMerger struct {
	responsesChannel <-chan BackendResponse
}

func newResponseHandler(ch <-chan BackendResponse) responsePicker {
	return &responseMerger{responsesChannel: ch}
}

func isSuccess(response BackendResponse) bool {
	if response.Error != nil || !response.IsSuccessful() {
		return false
	}
	return true
}

func (rm *responseMerger) createResponse(firstResponse BackendResponse, successes []BackendResponse) (resp *http.Response, err error) {
	reqQuery := firstResponse.Request.URL.Query()
	if reqQuery["location"] != nil {
		if len(successes) > 1 {
			for _, success := range successes[1:] {
				success.DiscardBody()
			}
		}
		return firstResponse.Response, firstResponse.Error
	}

	if reqQuery.Get("list-type") == listTypeV2 {
		log.Println("Create response v2", len(successes))

		return merger.MergeBucketListV2Responses(successes)
	}

	if reqQuery["versions"] != nil {
		return merger.MergeVersionsResponses(successes)
	}
	return merger.MergeBucketListResponses(successes)
}

func (rm *responseMerger) merge(firstTuple BackendResponse, rtupleCh <-chan BackendResponse) BackendResponse {
	successes := []BackendResponse{}
	if isSuccess(firstTuple) {
		successes = append(successes, firstTuple)
	}

	for tuple := range rtupleCh {
		if isSuccess(tuple) {
			successes = append(successes, tuple)
		} else {
			tuple.DiscardBody()
		}
	}

	if len(successes) > 0 {
		if !isSuccess(firstTuple) {
			err := firstTuple.DiscardBody()
			if err != nil {
				log.Printf("DiscardBody on ignored response tuple error: %s", err)
			}
		}

		res, err := rm.createResponse(firstTuple, successes)
		return BackendResponse{
			Response: res,
			Error:    err,
		}
	}
	return firstTuple
}

var unsupportedQueryParamNames = []string{
	"acl",
	"uploads",
	"tags",
	"requestPayment",
	"replication",
	"policy",
	"notification",
	"metrics",
	"logging",
	"lifecycle",
	"inventory",
	"encryption",
	"cors",
	"analytics",
	"accelerate",
	"website",
}

func (rm *responseMerger) isMergable(req *http.Request) bool {
	path := req.URL.Path
	method := req.Method
	reqQuery := req.URL.Query()
	unsupportedQuery := false
	if reqQuery != nil {
		for _, key := range unsupportedQueryParamNames {
			if reqQuery[key] != nil {
				unsupportedQuery = true
				break
			}
		}
	}
	return !unsupportedQuery && (method == http.MethodGet) && isBucketPath(path)
}
func isBucketPath(path string) bool {
	trimmedPath := strings.Trim(path, "/")
	if trimmedPath == "" {
		return false
	}
	return len(strings.Split(trimmedPath, "/")) == 1
}

// Pick implements picker interface
func (rm *responseMerger) Pick() (*http.Response, error) {
	firstTuple := <-rm.responsesChannel
	if !rm.isMergable(firstTuple.Request) {
		return &http.Response{
			Request:    firstTuple.Request,
			StatusCode: http.StatusNotImplemented,
		}, nil
	}
	result := rm.merge(firstTuple, rm.responsesChannel)
	return result.Response, result.Error
}

// SendSyncLog implements picker interface
func (rm *responseMerger) SendSyncLog(*SyncSender) {}
