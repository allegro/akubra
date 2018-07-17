package storages

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/allegro/akubra/storages/merger/s3datatypes"

	"github.com/stretchr/testify/suite"
)

// xmlDecoder provide decoded value in xml.
func xmlDecoder(body io.Reader, v interface{}) error {
	d := xml.NewDecoder(body)
	return d.Decode(v)
}

func responseBuilder(prefixes []s3datatypes.CommonPrefix, contents s3datatypes.ObjectInfos, maxKeys int) (BackendResponse, error) {
	r, err := http.NewRequest(http.MethodGet, "/bucket", nil)
	q := r.URL.Query()
	q.Add("max-keys", fmt.Sprintf("%d", maxKeys))
	r.URL.RawQuery = q.Encode()
	if err != nil {
		return BackendResponse{Request: r}, err
	}

	resp := &http.Response{
		Request: r,
	}

	lbres := s3datatypes.ListBucketResult{
		CommonPrefixes: prefixes,
		Contents:       contents,
		Delimiter:      "/",
		IsTruncated:    false,
		Marker:         "",
		MaxKeys:        int64(maxKeys),
		Name:           "defaultbucket",
		NextMarker:     "",
		Prefix:         "prefix",
	}

	bodyBytes, err := xml.Marshal(lbres)

	buf := bytes.NewBuffer(bodyBytes)
	resp.Body = ioutil.NopCloser(buf)
	return BackendResponse{Response: resp, Error: nil, Backend: nil, Request: r}, err
}

func responseV2Builder(prefixes []s3datatypes.CommonPrefix, contents []s3datatypes.ObjectInfo, maxKeys int) (BackendResponse, error) {
	request, err := http.NewRequest(http.MethodGet, "/bucket", nil)
	queryParams := request.URL.Query()
	queryParams.Add("max-keys", fmt.Sprintf("%d", maxKeys))
	queryParams.Add("list-type", fmt.Sprintf("%d", 2))

	request.URL.RawQuery = queryParams.Encode()
	if err != nil {
		return BackendResponse{}, err
	}

	resp := &http.Response{
		Request: request,
	}

	lbres := s3datatypes.ListBucketV2Result{
		CommonPrefixes:    prefixes,
		Contents:          contents,
		Delimiter:         "/",
		IsTruncated:       false,
		MaxKeys:           int64(maxKeys),
		Name:              "defaultbucket",
		ContinuationToken: "",
		Prefix:            "prefix",
	}

	bodyBytes, err := xml.Marshal(lbres)

	buf := bytes.NewBuffer(bodyBytes)
	resp.Body = ioutil.NopCloser(buf)
	return BackendResponse{Response: resp, Error: nil, Backend: nil, Request: request}, err
}

func prefixes(prefix ...string) s3datatypes.CommonPrefixes {
	pfs := []s3datatypes.CommonPrefix{}
	for _, p := range prefix {
		pfs = append(pfs, s3datatypes.CommonPrefix{Prefix: p})
	}
	return pfs
}

func contents(key ...string) s3datatypes.ObjectInfos {
	ofs := []s3datatypes.ObjectInfo{}
	for _, k := range key {
		ofs = append(ofs, s3datatypes.ObjectInfo{Key: k})
	}
	return ofs
}

func readBucketList(resp *http.Response) s3datatypes.ListBucketResult {
	list := s3datatypes.ListBucketResult{}
	buf := bytes.Buffer{}
	_, err := buf.ReadFrom(resp.Body)
	if err != nil {
		return list
	}
	bbody := buf.Bytes()
	err = xmlDecoder(bytes.NewBuffer(bbody), &list)
	if err != nil {
		return list
	}
	return list
}

type BucketListResponseMergerTestSuite struct {
	suite.Suite
	storage  Storages
	rHandler responsePicker
	ch       chan BackendResponse
}

func (suite *BucketListResponseMergerTestSuite) Send(tup ...BackendResponse) {
	for _, t := range tup {
		suite.ch <- t
	}
	close(suite.ch)

}
func (suite *BucketListResponseMergerTestSuite) SetupTest() {
	suite.storage = Storages{Clusters: make(map[string]NamedCluster)}
	suite.ch = make(chan BackendResponse)
	merger := &responseMerger{responsesChannel: suite.ch}
	suite.rHandler = merger

}

func (suite *BucketListResponseMergerTestSuite) TestSingleResponseMerge() {
	maxKeys := 10
	ps := prefixes("Ala", "Kota", "Ma")
	cs := contents("a", "ale", "kot", "ma")

	tup1, err := responseBuilder(ps, cs, maxKeys)
	suite.NoError(err)
	go suite.Send(tup1)
	resp, err := suite.rHandler.Pick()

	suite.NoError(err)
	list := readBucketList(resp)
	suite.Equal(cs, list.Contents)
	suite.Equal(ps, list.CommonPrefixes)
}

func (suite *BucketListResponseMergerTestSuite) TestV2() {
	maxKeys := 10
	ps1 := prefixes("pa", "pz", "pb", "py")
	cs1 := contents("z", "a", "c", "y")

	ps2 := prefixes("ppa", "ppz", "ppb", "ppy")
	cs2 := contents("x", "b", "u", "w")

	tup1, err := responseV2Builder(ps1, cs1, maxKeys)
	suite.NoError(err)

	tup2, err := responseV2Builder(ps2, cs2, maxKeys)
	suite.NoError(err)

	go suite.Send(tup1, tup2)
	resp, err := suite.rHandler.Pick()

	suite.NoError(err)
	list := readBucketList(resp)
	suite.Equal(2, len(list.Contents))
	suite.Equal(cs1[1], list.Contents[0])
	suite.Equal(cs2[1], list.Contents[1])
	suite.Equal(8, len(list.CommonPrefixes))
}

func (suite *BucketListResponseMergerTestSuite) TestResponseMerge() {
	maxKeys := 10
	ps1 := prefixes("pa", "pz", "pb", "py")
	cs1 := contents("z", "a", "c", "y")

	ps2 := prefixes("ppa", "ppz", "ppb", "ppy")
	cs2 := contents("x", "b", "u", "w")

	tup1, err := responseBuilder(ps1, cs1, maxKeys)
	suite.NoError(err)

	tup2, err := responseBuilder(ps2, cs2, maxKeys)
	suite.NoError(err)

	go suite.Send(tup1, tup2)
	resp, err := suite.rHandler.Pick()

	suite.NoError(err)
	list := readBucketList(resp)
	suite.Equal(2, len(list.Contents))
	suite.Equal(cs1[1], list.Contents[0])
	suite.Equal(cs2[1], list.Contents[1])
	suite.Equal(8, len(list.CommonPrefixes))
}
func TestListMergerTestSuite(t *testing.T) {
	suite.Run(t, new(BucketListResponseMergerTestSuite))
}
