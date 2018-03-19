package storages

import (
	"net/http"
	"strings"

	"github.com/allegro/akubra/log"
	"github.com/allegro/akubra/storages/merger"
	"github.com/allegro/akubra/transport"
)

const listTypeV2 = "2"

type responseMerger struct {
	merger transport.MultipleResponsesHandler
}

func isSuccess(tup transport.ResErrTuple) bool {
	if tup.Err != nil || tup.Failed {
		return false
	}
	return true
}

func (rm *responseMerger) createResponse(firstTuple transport.ResErrTuple, successes []transport.ResErrTuple) (resp *http.Response, err error) {
	reqQuery := firstTuple.Req.URL.Query()
	log.Println("Create response")
	if reqQuery.Get("list-type") == listTypeV2 {
		log.Println("Create response v2", len(successes))

		return merger.MergeListV2Responses(successes)
	}

	if reqQuery["versions"] != nil {
		return merger.MergeVersionsResponses(successes)
	}

	return merger.MergeListResponses(successes)
}

func (rm *responseMerger) merge(firstTuple transport.ResErrTuple, rtupleCh <-chan transport.ResErrTuple) transport.ResErrTuple {
	successes := []transport.ResErrTuple{}
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
			firstTuple.DiscardBody()
		}

		res, err := rm.createResponse(firstTuple, successes)
		return transport.ResErrTuple{
			Res: res,
			Err: err,
		}
	}
	return firstTuple
}

var unsupportedQueryParamNames = []string{
	"acl",
	"uploads",
	// "list-type",
	// "versions",
	"tags",
	"requestPayment",
	"replication",
	"policy",
	"notification",
	"metrics",
	"logging",
	"location",
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

func (rm *responseMerger) responseHandler(in <-chan transport.ResErrTuple) transport.ResErrTuple {
	firstTuple := <-in
	req := firstTuple.Req
	if rm.isMergable(req) {
		return rm.merge(firstTuple, in)
	}

	// reqQuery := req.URL.Query()
	// if reqQuery.Get("list-type") == listTypeV2 {
	// 	return transport.ResErrTuple{
	// 		Req: req,
	// 		Res: &http.Response{
	// 			Request:    req,
	// 			StatusCode: http.StatusNotImplemented,
	// 		},
	// 	}
	// }

	inCopy := make(chan transport.ResErrTuple)
	go func() {
		inCopy <- firstTuple

		for tuple := range in {
			inCopy <- tuple
		}
		close(inCopy)
	}()
	return rm.merger(inCopy)
}
