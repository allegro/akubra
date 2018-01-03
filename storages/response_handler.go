package storages

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/allegro/akubra/log"
	"github.com/allegro/akubra/transport"
)

const listTypeV2 = "2"

type objectsContainer struct {
	set  map[string]struct{}
	list []ObjectInfo
}

func (oc *objectsContainer) append(obj ...ObjectInfo) {
	for _, o := range obj {
		if _, ok := oc.set[o.Key]; ok {
			continue
		}
		oc.set[o.Key] = struct{}{}
		oc.list = append(oc.list, o)
	}
}

func (oc *objectsContainer) Less(i, j int) bool { return oc.list[i].Key < oc.list[j].Key }

func (oc *objectsContainer) Len() int { return len(oc.list) }

func (oc *objectsContainer) Swap(i, j int) { oc.list[i], oc.list[j] = oc.list[j], oc.list[i] }

func (oc *objectsContainer) first(limit int) []ObjectInfo {
	sort.Sort(oc)
	if limit >= len(oc.list) {
		return oc.list
	}
	return oc.list[0:limit]
}

type prefixContainer struct {
	set  map[string]struct{}
	list []CommonPrefix
}

func (pc *prefixContainer) append(obj ...CommonPrefix) {
	for _, o := range obj {
		if _, ok := pc.set[o.Prefix]; ok {
			continue
		}
		pc.set[o.Prefix] = struct{}{}
		pc.list = append(pc.list, o)
	}
}

func (pc *prefixContainer) Less(i, j int) bool { return pc.list[i].Prefix < pc.list[j].Prefix }

func (pc *prefixContainer) Len() int { return len(pc.list) }

func (pc *prefixContainer) Swap(i, j int) { pc.list[i], pc.list[j] = pc.list[j], pc.list[i] }

func (pc *prefixContainer) first(limit int) []CommonPrefix {
	sort.Sort(pc)
	if limit >= len(pc.list) {
		return pc.list
	}
	return pc.list[0:limit]
}
func isBucketPath(path string) bool {
	trimmedPath := strings.Trim(path, "/")
	if trimmedPath == "" {
		return false
	}
	return len(strings.Split(trimmedPath, "/")) == 1
}

type responseMerger struct {
	merger transport.MultipleResponsesHandler
}

func isSuccess(tup transport.ResErrTuple) bool {
	if tup.Err != nil || tup.Failed {
		return false
	}
	return true
}

func pickResultSet(os objectsContainer, ps prefixContainer, maxKeys int, lbr ListBucketResult) ListBucketResult {
	lbr.CommonPrefixes = ps.first(maxKeys)
	oLen := maxKeys - len(lbr.CommonPrefixes)
	lbr.Contents = os.first(oLen)
	isTruncated := os.Len()+ps.Len() > maxKeys
	if !isTruncated {
		return lbr
	}
	if oLen > 0 {
		lbr.NextMarker = lbr.Contents[len(lbr.Contents)-1].Key
	} else {
		lbr.NextMarker = lbr.CommonPrefixes[len(lbr.CommonPrefixes)-1].Prefix
	}
	lbr.IsTruncated = isTruncated
	return lbr
}

func extractResults(resp *http.Response) ListBucketResult {

	lbr := ListBucketResult{}

	if resp.Body == nil {
		return lbr
	}

	buf := &bytes.Buffer{}

	if _, rerr := buf.ReadFrom(resp.Body); rerr != nil {
		log.Debug("Problem reading ObjectStore response body, %s", rerr)
		return lbr
	}

	if cerr := resp.Body.Close(); cerr != nil {
		log.Debug("Problem closing ObjectStore response body, %s", cerr)
		return lbr
	}

	bodyBytes := buf.Bytes()
	err := xml.Unmarshal(bodyBytes, &lbr)
	if err != nil {
		log.Debug("ListBucketResult unmarshalling problem %s", err)
	}
	return lbr
}

func (rm *responseMerger) createResponse(successes []transport.ResErrTuple) (resp *http.Response, err error) {
	if len(successes) == 0 {
		err = fmt.Errorf("No successful responses")
		return
	}
	oContainer := objectsContainer{
		list: make([]ObjectInfo, 0),
		set:  make(map[string]struct{}),
	}
	pContainer := prefixContainer{
		list: make([]CommonPrefix, 0),
		set:  make(map[string]struct{}),
	}
	var listBucketResult ListBucketResult
	for _, tuple := range successes {
		resp = tuple.Res
		listBucketResult = extractResults(resp)
		oContainer.append(listBucketResult.Contents...)
		pContainer.append(listBucketResult.CommonPrefixes...)
	}

	req := successes[0].Res.Request
	reqQuery := req.URL.Query()
	maxKeysQuery := reqQuery.Get("max-keys")
	maxKeys, err := strconv.Atoi(maxKeysQuery)
	if err != nil {
		maxKeys = 1000
	}
	listBucketResult = pickResultSet(oContainer, pContainer, maxKeys, listBucketResult)

	bodyBytes, err := xml.Marshal(listBucketResult)
	if err != nil {
		log.Debug("Problem marshalling ObjectStore response body, %s", err)
		return nil, err
	}
	buf := bytes.NewBuffer(bodyBytes)
	resp.Body = ioutil.NopCloser(buf)
	resp.ContentLength = int64(buf.Len())
	resp.Header = http.Header{}
	resp.Header.Set("content-length", fmt.Sprintf("%d", buf.Len()))
	resp.Header.Set("content-type", "application/xml")
	return resp, nil
}

func (rm *responseMerger) merge(firstTuple transport.ResErrTuple, rtupleCh <-chan transport.ResErrTuple) transport.ResErrTuple {
	successes := []transport.ResErrTuple{}
	if isSuccess(firstTuple) {
		successes = append(successes, firstTuple)
	} else {
		firstTuple.DiscardBody()
	}
	for tuple := range rtupleCh {
		if isSuccess(tuple) {
			successes = append(successes, tuple)
		} else {
			tuple.DiscardBody()
		}
	}
	if len(successes) > 0 {
		res, err := rm.createResponse(successes)
		return transport.ResErrTuple{
			Res: res,
			Err: err,
		}
	}
	return firstTuple
}

func (rm *responseMerger) responseHandler(in <-chan transport.ResErrTuple) transport.ResErrTuple {
	firstTuple := <-in
	path := firstTuple.Req.URL.Path
	method := firstTuple.Req.Method

	if method != http.MethodGet || !isBucketPath(path) {
		reqQuery := firstTuple.Req.URL.Query()
		if reqQuery.Get("list-type") == listTypeV2 {
			return transport.ResErrTuple{
				Req: firstTuple.Req,
				Res: &http.Response{
					Request:    firstTuple.Req,
					StatusCode: http.StatusNotImplemented,
				},
			}
		}
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
	return rm.merge(firstTuple, in)
}
