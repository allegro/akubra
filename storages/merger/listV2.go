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

func MergeListV2Responses(successes []transport.ResErrTuple) (resp *http.Response, err error) {
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
	var listBucketResult s3datatypes.ListBucketV2Result
	for _, tuple := range successes {
		resp = tuple.Res
		listBucketResult = extractListv2Results(resp)
		oContainer.append(listBucketResult.Contents.ToStringer()...)
		pContainer.append(listBucketResult.CommonPrefixes.ToStringer()...)
		log.Debugf("len oContainer %v", oContainer.list)
		log.Debugf("len pContainer %v", pContainer.list)
	}

	req := successes[0].Res.Request
	reqQuery := req.URL.Query()
	maxKeysQuery := reqQuery.Get("max-keys")
	maxKeys, err := strconv.Atoi(maxKeysQuery)
	if err != nil {
		maxKeys = 1000
	}

	listBucketResult = pickListV2ResultSet(oContainer, pContainer, maxKeys, listBucketResult)

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

func pickListV2ResultSet(os objectsContainer, ps objectsContainer, maxKeys int, lbr s3datatypes.ListBucketV2Result) s3datatypes.ListBucketV2Result {
	lbr.CommonPrefixes = lbr.CommonPrefixes.FromStringer(ps.first(maxKeys))
	oLen := maxKeys - len(lbr.CommonPrefixes)
	log.Debugf("%v %d", os.first(oLen), oLen)
	lbr.Contents = lbr.Contents.FromStringer(os.first(oLen))
	isTruncated := os.Len()+ps.Len() > maxKeys
	if !isTruncated {
		return lbr
	}
	// TODO-mj: pack NextContinuatio
	lbr.IsTruncated = isTruncated
	return lbr
}
