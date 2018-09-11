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
	if rm.isPartiallyMergable(firstResponse.Request) {
		return merger.MergePartially(firstResponse, successes)
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
			if err := tuple.DiscardBody(); err != nil {
				log.Printf("Could not discard tuple body, %s", err)
			}
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
	"uploads",
}

var partialSupportQueryParamNames = []string{"acl",
	"accelerate",
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

func (rm *responseMerger) isPartiallyMergable(req *http.Request) bool {
	path := req.URL.Path
	method := req.Method
	reqQuery := req.URL.Query()
	partiallySupportedQuery := false
	if reqQuery != nil {
		for _, key := range partialSupportQueryParamNames {
			if reqQuery[key] != nil {
				partiallySupportedQuery = true
				break
			}
		}
	}
	return partiallySupportedQuery && (method == http.MethodGet) && isBucketPath(path)
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
		log.Debugf("RequestUnmergable path: %s, method: %s, query:%s, id: %s",
			firstTuple.Request.URL.Path,
			firstTuple.Request.Method,
			firstTuple.Request.URL.RawQuery,
			firstTuple.ReqID(),
		)
		go func(rch <-chan BackendResponse) {
			for br := range rch {
				if err := br.DiscardBody(); err != nil {
					log.Debugf("Could not close tuple body: %s", err)
				}
			}
		}(rm.responsesChannel)
		if err := firstTuple.DiscardBody(); err != nil {
			log.Debugf("Could not close tuple body: %s", err)
		}

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
