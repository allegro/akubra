package merger

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/allegro/akubra/log"
	"github.com/allegro/akubra/storages/merger/s3datatypes"
	"github.com/allegro/akubra/transport"
)

// MergeBucketListV2Responses unifies responses from multiple backends
func MergeBucketListV2Responses(successes []transport.ResErrTuple) (resp *http.Response, err error) {
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
	var listBucketV2Result s3datatypes.ListBucketV2Result
	for _, tuple := range successes {
		resp = tuple.Res
		listBucketV2Result = extractListv2Results(resp)
		keys.append(listBucketV2Result.Contents.ToStringer()...)
		prefixes.append(listBucketV2Result.CommonPrefixes.ToStringer()...)
	}

	req := successes[0].Res.Request
	reqQuery := req.URL.Query()
	maxKeysQuery := reqQuery.Get("max-keys")
	maxKeys, err := strconv.Atoi(maxKeysQuery)
	if err != nil {
		maxKeys = 1000
	}

	listBucketV2Result = pickListV2ResultSet(keys, prefixes, maxKeys, listBucketV2Result)

	bodyBytes, err := xml.Marshal(listBucketV2Result)
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

func extractListv2Results(resp *http.Response) s3datatypes.ListBucketV2Result {
	lbr := s3datatypes.ListBucketV2Result{}
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

func pickListV2ResultSet(keys objectsContainer, prefixes objectsContainer, maxKeys int, bucketListV2Result s3datatypes.ListBucketV2Result) s3datatypes.ListBucketV2Result {
	bucketListV2Result.CommonPrefixes = bucketListV2Result.CommonPrefixes.FromStringer(prefixes.first(maxKeys))
	keysCount := maxKeys - len(bucketListV2Result.CommonPrefixes)
	bucketListV2Result.Contents = bucketListV2Result.Contents.FromStringer(keys.first(keysCount))
	isTruncated := keys.Len()+prefixes.Len() > maxKeys
	if !isTruncated {
		return bucketListV2Result
	}
	// TODO-mj: pack NextContinuatio
	if keysCount > 0 {
		bucketListV2Result.ContinuationToken = bucketListV2Result.Contents[len(bucketListV2Result.Contents)-1].Key
	} else {
		bucketListV2Result.ContinuationToken = bucketListV2Result.CommonPrefixes[len(bucketListV2Result.CommonPrefixes)-1].Prefix
	}
	bucketListV2Result.IsTruncated = isTruncated
	return bucketListV2Result
}

type interceptor struct {
	rt http.RoundTripper
}

const listTypeV2 = "2"

func (i *interceptor) RoundTrip(req *http.Request) (*http.Response, error) {
	reqQuery := req.URL.Query()
	if reqQuery.Get("list-type") == listTypeV2 && len(reqQuery.Get("continuation-token")) > 0 {
		reqQuery.Set("start-after", reqQuery.Get("continuation-token"))
		reqQuery.Del("continuation-token")
		req.URL.RawQuery = reqQuery.Encode()
	}
	return i.rt.RoundTrip(req)
}

// ListV2Interceptor rewrites listV2 query params
func ListV2Interceptor(roundTripper http.RoundTripper) http.RoundTripper {
	return &interceptor{rt: roundTripper}
}
