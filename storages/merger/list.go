package merger

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"net/http"
	"sort"
	"strconv"

	"github.com/allegro/akubra/log"
	"github.com/allegro/akubra/storages/merger/s3datatypes"
	"github.com/allegro/akubra/transport"
)

func MergeListResponses(successes []transport.ResErrTuple) (resp *http.Response, err error) {
	if len(successes) == 0 {
		log.Printf("No successful response")
		err = fmt.Errorf("No successful responses")
		return
	}
	oContainer := objectsContainer{
		list: make([]fmt.Stringer, 0),
		set:  make(map[string]struct{}),
	}
	pContainer := objectsContainer{
		list: make([]fmt.Stringer, 0),
		set:  make(map[string]struct{}),
	}
	var listBucketResult s3datatypes.ListBucketResult
	for _, tuple := range successes {
		resp = tuple.Res
		listBucketResult = extractListResults(resp)
		oContainer.append(listBucketResult.Contents.ToStringer()...)
		pContainer.append(listBucketResult.CommonPrefixes.ToStringer()...)
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

type objectsContainer struct {
	set  map[string]struct{}
	list []fmt.Stringer
}

func (oc *objectsContainer) append(obj ...fmt.Stringer) {
	for _, o := range obj {
		if _, ok := oc.set[o.String()]; ok {
			continue
		}
		oc.set[o.String()] = struct{}{}
		oc.list = append(oc.list, o)
	}
}

func (oc *objectsContainer) Less(i, j int) bool { return oc.list[i].String() < oc.list[j].String() }

func (oc *objectsContainer) Len() int { return len(oc.list) }

func (oc *objectsContainer) Swap(i, j int) { oc.list[i], oc.list[j] = oc.list[j], oc.list[i] }

func (oc *objectsContainer) first(limit int) []fmt.Stringer {
	sort.Sort(oc)
	if limit >= len(oc.list) {
		return oc.list
	}
	log.Debug("returning %d elements, has %d", limit, len(oc.list))
	return oc.list[0:limit]
}

func extractListResults(resp *http.Response) s3datatypes.ListBucketResult {
	lbr := s3datatypes.ListBucketResult{}
	if resp.Body == nil {
		return lbr
	}

	buf := &bytes.Buffer{}
	if _, rerr := buf.ReadFrom(resp.Body); rerr != nil {
		log.Debugf("Problem reading ObjectStore response body, %s", rerr)
		return lbr
	}

	if cerr := resp.Body.Close(); cerr != nil {
		log.Debugf("Problem closing ObjectStore response body, %s", cerr)
		return lbr
	}

	bodyBytes := buf.Bytes()
	err := xml.Unmarshal(bodyBytes, &lbr)
	if err != nil {
		log.Debugf("ListBucketResult unmarshalling problem %s", err)
	}

	return lbr
}

func pickResultSet(os objectsContainer, ps objectsContainer, maxKeys int, lbr s3datatypes.ListBucketResult) s3datatypes.ListBucketResult {
	lbr.CommonPrefixes.FromStringer(ps.first(maxKeys))
	oLen := maxKeys - len(lbr.CommonPrefixes)
	log.Println("oLen", oLen, maxKeys)
	lbr.Contents.FromStringer(os.first(oLen))
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
