package storages

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/allegro/akubra/log"
	"github.com/allegro/akubra/transport"
	minio "github.com/mjarco/minio-go"
)

type objectsContainer struct {
	set  map[string]struct{}
	list []minio.ObjectInfo
}

func (oc *objectsContainer) append(obj ...minio.ObjectInfo) {
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

func (oc *objectsContainer) first(limit int) []minio.ObjectInfo {
	sort.Sort(oc)
	if limit >= len(oc.list) {
		return oc.list
	}
	return oc.list[0:limit]
}

type prefixContainer struct {
	set  map[string]struct{}
	list []minio.CommonPrefix
}

func (pc *prefixContainer) append(obj ...minio.CommonPrefix) {
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

func (pc *prefixContainer) first(limit int) []minio.CommonPrefix {
	sort.Sort(pc)
	if limit >= len(pc.list) {
		return pc.list
	}
	return pc.list[0:limit]
}
func isBucketPath(path string) bool {
	trimmedPath := strings.Trim(path, "/")
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

// xmlDecoder provide decoded value in xml.
func xmlDecoder(body io.Reader, v interface{}) error {
	d := xml.NewDecoder(body)
	return d.Decode(v)
}

func pickResultSet(os objectsContainer, ps prefixContainer, maxKeys int, lbr minio.ListBucketResult) minio.ListBucketResult {
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

func (rm *responseMerger) createResponse(successes []transport.ResErrTuple) (resp *http.Response, err error) {
	if len(successes) == 0 {
		err = fmt.Errorf("No successful responses")
		return
	}
	oContainer := objectsContainer{
		list: make([]minio.ObjectInfo, 0),
		set:  make(map[string]struct{}, 0),
	}
	pContainer := prefixContainer{
		list: make([]minio.CommonPrefix, 0),
		set:  make(map[string]struct{}, 0),
	}
	var listBucketResult minio.ListBucketResult
	for _, tuple := range successes {
		resp = tuple.Res

		listBucketResult = minio.ListBucketResult{}
		buf := &bytes.Buffer{}
		buf.ReadFrom(resp.Body)
		bodyBytes := buf.Bytes()
		err = xml.Unmarshal(bodyBytes, &listBucketResult)
		if err != nil {
			return nil, err
		}
		oContainer.append(listBucketResult.Contents...)
		pContainer.append(listBucketResult.CommonPrefixes...)
	}

	req := successes[0].Res.Request
	reqQuery := req.URL.Query()
	maxKeysQuery := reqQuery.Get("max-keys")
	maxKeys, err := strconv.Atoi(maxKeysQuery)
	if err != nil {
		log.Println(err)
		maxKeys = 1000
	}
	listBucketResult = pickResultSet(oContainer, pContainer, maxKeys, listBucketResult)
	// TODO Buffer and io.ReadCloser
	// pass to response body
	buf := &bytes.Buffer{}
	enc := xml.NewEncoder(buf)
	enc.Encode(listBucketResult)
	enc.Flush()
	resp.Body = ioutil.NopCloser(buf)
	resp.ContentLength = int64(buf.Len())
	resp.Header = http.Header{}
	resp.Header.Set("content-length", fmt.Sprintf("%d", buf.Len()))
	resp.Header.Set("content-type", "application/xml")
	return resp, nil
}

func (rm *responseMerger) merge(firstTuple transport.ResErrTuple, rtupleCh <-chan transport.ResErrTuple) transport.ResErrTuple {
	successes := []transport.ResErrTuple{}
	errors := []transport.ResErrTuple{}
	if isSuccess(firstTuple) {
		successes = append(successes, firstTuple)
	} else {
		errors = append(errors, firstTuple)
	}
	for tuple := range rtupleCh {
		if isSuccess(tuple) {
			successes = append(successes, tuple)
		} else {
			errors = append(errors, tuple)
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
	path := firstTuple.Res.Request.URL.Path
	method := firstTuple.Res.Request.Method
	if method != http.MethodGet || !isBucketPath(path) {
		inCopy := make(chan transport.ResErrTuple)
		inCopy <- firstTuple
		go func() {
			for tuple := range in {
				inCopy <- tuple
			}
			close(inCopy)
		}()
		return rm.merger(inCopy)
	}
	return rm.merge(firstTuple, in)
}
