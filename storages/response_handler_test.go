package storages

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/allegro/akubra/transport"
	minio "github.com/minio/minio-go"

	"github.com/stretchr/testify/suite"
)

func responseBuilder(prefixes []minio.CommonPrefix, contents []minio.ObjectInfo, maxKeys int) (*http.Response, error) {
	r, err := http.NewRequest(http.MethodGet, "/bucket", nil)
	q := r.URL.Query()
	q.Add("max-keys", fmt.Sprintf("%d", maxKeys))
	r.URL.RawQuery = q.Encode()
	if err != nil {
		return nil, err
	}

	resp := &http.Response{
		Request: r,
	}

	lbres := minio.ListBucketResult{
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
	buf := &bytes.Buffer{}
	enc := xml.NewEncoder(buf)
	err = enc.Encode(lbres)
	enc.Flush()
	resp.Body = ioutil.NopCloser(buf)
	return resp, err
}

func prefixes(prefix ...string) []minio.CommonPrefix {
	pfs := []minio.CommonPrefix{}
	for _, p := range prefix {
		pfs = append(pfs, minio.CommonPrefix{Prefix: p})
	}
	return pfs
}

func contents(key ...string) []minio.ObjectInfo {
	ofs := []minio.ObjectInfo{}
	for _, k := range key {
		ofs = append(ofs, minio.ObjectInfo{Key: k})
	}
	return ofs
}

func readBucketList(resp *http.Response) minio.ListBucketResult {
	list := minio.ListBucketResult{}
	buf := bytes.Buffer{}
	buf.ReadFrom(resp.Body)
	bbody := buf.Bytes()
	xmlDecoder(bytes.NewBuffer(bbody), &list)
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

	tup1 := transport.ResErrTuple{}
	tup1.Res, _ = responseBuilder(ps, cs, maxKeys)

	go suite.Send(tup1)
	rtup := suite.rHandler(suite.ch)

	suite.NoError(rtup.Err)
	list := readBucketList(rtup.Res)
	suite.Equal(cs, list.Contents)
	suite.Equal(ps, list.CommonPrefixes)
}

func (suite *BucketListResponseMergerTestSuite) TestResponseMerge() {
	maxKeys := 10
	ps1 := prefixes("pa", "pz", "pb", "py")
	cs1 := contents("z", "a", "c", "y")

	ps2 := prefixes("ppa", "ppz", "ppb", "ppy")
	cs2 := contents("x", "b", "u", "w")

	tup1 := transport.ResErrTuple{}
	tup1.Res, _ = responseBuilder(ps1, cs1, maxKeys)

	tup2 := transport.ResErrTuple{}
	tup2.Res, _ = responseBuilder(ps2, cs2, maxKeys)

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
