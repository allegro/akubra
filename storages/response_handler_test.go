package storages

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/allegro/akubra/transport"

	"github.com/stretchr/testify/suite"
)

// xmlDecoder provide decoded value in xml.
func xmlDecoder(body io.Reader, v interface{}) error {
	d := xml.NewDecoder(body)
	return d.Decode(v)
}

func responseBuilder(prefixes []CommonPrefix, contents []ObjectInfo, maxKeys int) (transport.ResErrTuple, error) {
	r, err := http.NewRequest(http.MethodGet, "/bucket", nil)
	q := r.URL.Query()
	q.Add("max-keys", fmt.Sprintf("%d", maxKeys))
	r.URL.RawQuery = q.Encode()
	if err != nil {
		return transport.ResErrTuple{}, err
	}

	resp := &http.Response{
		Request: r,
	}

	lbres := ListBucketResult{
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
	return transport.ResErrTuple{Req: r, Res: resp, Err: nil, Failed: false}, err
}

func prefixes(prefix ...string) []CommonPrefix {
	pfs := []CommonPrefix{}
	for _, p := range prefix {
		pfs = append(pfs, CommonPrefix{Prefix: p})
	}
	return pfs
}

func contents(key ...string) []ObjectInfo {
	ofs := []ObjectInfo{}
	for _, k := range key {
		ofs = append(ofs, ObjectInfo{Key: k})
	}
	return ofs
}

func readBucketList(resp *http.Response) ListBucketResult {
	list := ListBucketResult{}
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
	rHandler transport.MultipleResponsesHandler
	ch       chan transport.ResErrTuple
}

func (suite *BucketListResponseMergerTestSuite) Send(tup ...transport.ResErrTuple) {
	for _, t := range tup {
		suite.ch <- t
	}
	close(suite.ch)

}
func (suite *BucketListResponseMergerTestSuite) SetupTest() {
	suite.storage = Storages{Clusters: make(map[string]NamedCluster)}
	merger := responseMerger{}
	suite.rHandler = merger.responseHandler
	suite.ch = make(chan transport.ResErrTuple)
}

func (suite *BucketListResponseMergerTestSuite) TestSingleResponseMerge() {
	maxKeys := 10
	ps := prefixes("Ala", "Kota", "Ma")
	cs := contents("a", "ale", "kot", "ma")

	tup1, err := responseBuilder(ps, cs, maxKeys)
	suite.NoError(err)
	go suite.Send(tup1)
	rtup := suite.rHandler(suite.ch)

	suite.NoError(rtup.Err)
	list := readBucketList(rtup.Res)
	suite.Equal(cs, list.Contents)
	suite.Equal(ps, list.CommonPrefixes)
}

func (suite *BucketListResponseMergerTestSuite) TestV2NotImplemented() {
	req, err := http.NewRequest("Get", "http://localhost:8080/bucket", nil)
	suite.NoError(err)

	query := req.URL.Query()
	query.Set("list-type", "2")
	req.URL.RawQuery = query.Encode()
	tup1 := transport.ResErrTuple{
		Req: req,
		Res: &http.Response{
			Request:    req,
			StatusCode: http.StatusNoContent,
		},
	}

	suite.NoError(err)
	go suite.Send(tup1)
	rtup := suite.rHandler(suite.ch)

	suite.NoError(rtup.Err)
	suite.Equal(rtup.Res.StatusCode, http.StatusNotImplemented)
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
	rtup := suite.rHandler(suite.ch)

	suite.NoError(rtup.Err)
	list := readBucketList(rtup.Res)
	suite.Equal(2, len(list.Contents))
	suite.Equal(cs1[1], list.Contents[0])
	suite.Equal(cs2[1], list.Contents[1])
	suite.Equal(8, len(list.CommonPrefixes))
}
func TestListMergerTestSuite(t *testing.T) {
	suite.Run(t, new(BucketListResponseMergerTestSuite))
}
