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
	"github.com/allegro/akubra/storages/backend"
	"github.com/allegro/akubra/storages/merger/s3datatypes"
)

// MergeBucketListResponses unifies responses from multiple backends
func MergeBucketListResponses(successes []backend.Response) (resp *http.Response, err error) {
	if len(successes) == 0 {
		err = fmt.Errorf("No successful responses")
		return
	}
	keys := objectsContainer{
		list: make([]fmt.Stringer, 0),
		set:  make(map[string]struct{}),
	}
	prefixes := objectsContainer{
		list: make([]fmt.Stringer, 0),
		set:  make(map[string]struct{}),
	}
	var listBucketResult s3datatypes.ListBucketResult
	for _, tuple := range successes {
		resp = tuple.Response
		listBucketResult = extractListResults(resp)
		keys.append(listBucketResult.Contents.ToStringer()...)
		prefixes.append(listBucketResult.CommonPrefixes.ToStringer()...)
		discardErr := tuple.DiscardBody()
		if discardErr != nil {
			log.Debug("Response discard error in MergeBucketListResponses %s", discardErr)
		}
	}

	req := successes[0].Request
	reqQuery := req.URL.Query()
	maxKeysQuery := reqQuery.Get("max-keys")
	maxKeys, err := strconv.Atoi(maxKeysQuery)
	if err != nil {
		maxKeys = 1000
	}

	listBucketResult = createResultSet(keys, prefixes, maxKeys, listBucketResult)

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

func (oc *objectsContainer) Swap(i, j int)          { oc.list[i], oc.list[j] = oc.list[j], oc.list[i] }
func (oc *objectsContainer) Get(i int) fmt.Stringer { return oc.list[i] }

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

	bodyBytes := buf.Bytes()
	err := xml.Unmarshal(bodyBytes, &lbr)
	if err != nil {
		log.Debugf("ListBucketResult unmarshalling problem %s", err)
	}

	return lbr
}

func createResultSet(keys objectsContainer, prefixes objectsContainer, maxKeys int, listBucketResult s3datatypes.ListBucketResult) s3datatypes.ListBucketResult {
	listBucketResult.CommonPrefixes = listBucketResult.CommonPrefixes.FromStringer(prefixes.first(maxKeys))
	keysCount := maxKeys - len(listBucketResult.CommonPrefixes)
	listBucketResult.Contents = listBucketResult.Contents.FromStringer(keys.first(keysCount))
	listBucketResult.IsTruncated = listBucketResult.IsTruncated || keys.Len()+prefixes.Len() > maxKeys
	if listBucketResult.IsTruncated {
		if keysCount > 0 {
			listBucketResult.NextMarker = listBucketResult.Contents[len(listBucketResult.Contents)-1].Key
		} else {
			listBucketResult.NextMarker = listBucketResult.CommonPrefixes[len(listBucketResult.CommonPrefixes)-1].Prefix
		}
	}
	return listBucketResult
}
