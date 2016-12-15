package sharding

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sync/atomic"
	"testing"

	"github.com/allegro/akubra/config"
	"github.com/allegro/akubra/httphandler"
	set "github.com/deckarep/golang-set"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mkDummySrvsWithfun(count int, t *testing.T, handlerfunc func(w http.ResponseWriter, r *http.Request)) []config.YAMLURL {
	urls := make([]config.YAMLURL, 0, count)
	dummySrvs := make([]*httptest.Server, 0, count)
	for i := 0; i < count; i++ {
		handlerfun := http.HandlerFunc(handlerfunc)
		ts := httptest.NewServer(handlerfun)
		dummySrvs = append(dummySrvs, ts)
		urlN, err := url.Parse(ts.URL)
		if err != nil {
			t.Error(err)
		}
		urls = append(urls, config.YAMLURL{URL: urlN})
	}
	return urls
}

func mkDummySrvs(count int, stream []byte, t *testing.T) []config.YAMLURL {
	f := func(w http.ResponseWriter, r *http.Request) {
		_, err := w.Write(stream)
		assert.Nil(t, err)
	}
	return mkDummySrvsWithfun(count, t, f)
}

var defaultClusterConfig = config.ClusterConfig{
	Type:    "replicator",
	Weight:  1,
	Options: map[string]string{},
}

func configure(backends []config.YAMLURL) config.Config {

	timeout := "3s"
	connLimit := int64(10)
	methodsSlice := []string{"PUT", "GET", "DELETE"}

	methodsSet := set.NewThreadUnsafeSet()
	for _, method := range methodsSlice {
		methodsSet.Add(method)
	}

	syncLogger := log.New(os.Stdout, "sync: ", log.Lshortfile)
	accessLogger := log.New(os.Stdout, "accs: ", log.Lshortfile)
	mainLogger := log.New(os.Stdout, "main: ", log.Lshortfile)
	defaultClusterConfig.Backends = backends

	clustersConf := make(map[string]config.ClusterConfig)
	clustersConf["cluster1"] = defaultClusterConfig

	clientCfg := config.ClientConfig{
		Name:        "client1",
		Clusters:    []string{"cluster1"},
		ShardsCount: 20,
	}

	return config.Config{
		YamlConfig: config.YamlConfig{
			ConnLimit:             connLimit,
			ConnectionTimeout:     timeout,
			ConnectionDialTimeout: timeout,
			Client:                clientCfg,
			Clusters:              clustersConf,
			Backends:              backends,
		},
		SyncLogMethodsSet: methodsSet,
		Synclog:           syncLogger,
		Accesslog:         accessLogger,
		Mainlog:           mainLogger,
	}
}

func makeRingFactory(conf config.Config) ringFactory {
	httptransp := httphandler.ConfigureHTTPTransport(conf)
	respHandler := httphandler.NewMultipleResponseHandler(conf)
	return newRingFactory(conf, httptransp, respHandler)
}

func TestSingleClusterOnRing(t *testing.T) {
	stream := []byte("cluster1")
	cluster1Urls := mkDummySrvs(2, stream, t)
	conf := configure(cluster1Urls)
	ringFactory := makeRingFactory(conf)
	clientRing, err := ringFactory.clientRing(conf.Client)
	require.NoError(t, err)
	req, _ := http.NewRequest("GET", "http://example.com/f/a", nil)
	resp, err := clientRing.RoundTrip(req)
	require.NoError(t, err)
	respBody := make([]byte, resp.ContentLength)
	_, err = io.ReadFull(resp.Body, respBody)
	require.NoError(t, err)
	assert.Equal(t, stream, respBody)
}

func TestTwoClustersOnRing(t *testing.T) {
	response1 := []byte("aaa")
	cluster1Urls := mkDummySrvs(2, response1, t)
	response2 := []byte("bbb")
	cluster2Urls := mkDummySrvs(2, response2, t)
	conf := configure(cluster1Urls)
	conf.Clusters["test"] = config.ClusterConfig{
		Weight:   1,
		Type:     "replicator",
		Backends: cluster2Urls,
	}

	conf.Client.Clusters = append(conf.Client.Clusters, "test")

	ringFactory := makeRingFactory(conf)

	clientRing, err := ringFactory.clientRing(conf.Client)
	require.NoError(t, err)
	reader := bytes.NewBuffer([]byte{})
	URL := "http://example.com/myindex/abcdef"
	req, _ := http.NewRequest("PUT", URL, reader)
	resp, err := clientRing.RoundTrip(req)
	require.NoError(t, err)

	respBody := make([]byte, 3)
	_, err = io.ReadFull(resp.Body, respBody)
	require.NoError(t, err, "cannot read response")
	assert.Equal(t, response1, respBody, "response differs")

	req2, _ := http.NewRequest("PUT", "http://example.com/myindex/a", reader)
	resp2, err2 := clientRing.RoundTrip(req2)
	require.NoError(t, err2)

	respBody2 := make([]byte, 3)
	_, err = io.ReadFull(resp2.Body, respBody2)
	require.NoError(t, err, "cannot read response")
	assert.Equal(t, response2, respBody2, "response differs")
}

func TestBucketOpDetection(t *testing.T) {
	sr := shardsRing{}
	testCases := []struct {
		path     string
		expected bool
	}{
		{"/foo", true},
		{"/bar/", true},
		{"/foo/1", false},
		{"/bar/1/", false},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s is bucket path", tc.path), func(t *testing.T) {
			assert.Equal(t, sr.isBucketPath(tc.path), tc.expected)
		})
	}
}

func TestTwoClustersOnRingBucketOp(t *testing.T) {
	callCount := int64(0)
	f := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		atomic.AddInt64(&callCount, 1)
	}

	cluster1Urls := mkDummySrvsWithfun(2, t, f)
	conf := configure(cluster1Urls)
	cluster2Urls := mkDummySrvsWithfun(2, t, f)
	conf.Clusters["test"] = config.ClusterConfig{
		Weight:   1,
		Type:     "replicator",
		Backends: cluster2Urls,
	}

	conf.Client.Clusters = append(conf.Client.Clusters, "test")
	ringFactory := makeRingFactory(conf)

	clientRing, err := ringFactory.clientRing(conf.Client)

	require.NoError(t, err)
	reader := bytes.NewBuffer([]byte{})
	req, _ := http.NewRequest("PUT", "http://example.com/index/", reader)
	_, err2 := clientRing.RoundTrip(req)
	require.NoError(t, err2)

	assert.Equal(t, int64(4), callCount, "No all backends called")
}

func TestTwoClustersOnRingBucketSharding(t *testing.T) {
	callCount := int64(0)
	f := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		atomic.AddInt64(&callCount, 1)
	}

	cluster1Urls := mkDummySrvsWithfun(2, t, f)
	conf := configure(cluster1Urls)
	cluster2Urls := mkDummySrvsWithfun(2, t, f)
	conf.Clusters["test"] = config.ClusterConfig{
		Weight:   1,
		Type:     "replicator",
		Backends: cluster2Urls,
	}

	conf.Client.Clusters = append(conf.Client.Clusters, "test")
	ringFactory := makeRingFactory(conf)
	clientRing, err := ringFactory.clientRing(conf.Client)
	require.NoError(t, err)

	reader := bytes.NewBuffer([]byte{})
	req, _ := http.NewRequest("PUT", "http://example.com/index/a", reader)

	_, err2 := clientRing.RoundTrip(req)
	require.NoError(t, err2)
	assert.Equal(t, int64(2), callCount, "Too many backends called")
}

func TestBacktracking(t *testing.T) {
	response := []byte("bbb")
	cluster1Urls := mkDummySrvs(2, response, t)

	conf := configure(cluster1Urls)

	cluster2Urls := mkDummySrvsWithfun(2, t, http.NotFound)

	conf.Clusters["test"] = config.ClusterConfig{
		Weight:   1,
		Type:     "replicator",
		Backends: cluster2Urls,
	}

	conf.Client.Clusters = append(conf.Client.Clusters, "test")
	ringFactory := makeRingFactory(conf)
	clientRing, err := ringFactory.clientRing(conf.Client)
	require.NoError(t, err)

	req, _ := http.NewRequest("GET", "http://example.com/index/a", nil)
	resp, err := clientRing.RoundTrip(req)
	require.NoError(t, err)

	assert.NotEqual(t, http.StatusNotFound, resp.StatusCode)
}

func TestDeletePassToAllBackends(t *testing.T) {
	callCount := int64(0)
	f := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		atomic.AddInt64(&callCount, 1)
	}

	cluster1Urls := mkDummySrvsWithfun(2, t, f)
	conf := configure(cluster1Urls)
	cluster2Urls := mkDummySrvsWithfun(2, t, f)
	conf.Clusters["test"] = config.ClusterConfig{
		Weight:   1,
		Type:     "replicator",
		Backends: cluster2Urls,
	}

	conf.Client.Clusters = append(conf.Client.Clusters, "test")
	ringFactory := makeRingFactory(conf)

	clientRing, err := ringFactory.clientRing(conf.Client)
	require.NoError(t, err)

	req, _ := http.NewRequest("DELETE", "http://example.com/index/a", nil)
	_, err2 := clientRing.RoundTrip(req)
	require.NoError(t, err2)
	assert.Equal(t, int64(4), callCount, "All backends should be called")

}

func TestBodyResend(t *testing.T) {
	callCount := int64(0)
	f10BErr := func(w http.ResponseWriter, r *http.Request) {
		read10Bytes := make([]byte, 10)
		n, err := io.ReadFull(r.Body, read10Bytes)
		assert.NoError(t, err)
		assert.Equal(t, 10, n, "Should read 10 bytes")
		assert.NoError(t, r.Body.Close())
		w.WriteHeader(http.StatusTeapot)
		atomic.AddInt64(&callCount, 1)
	}

	fReadAllOk := func(w http.ResponseWriter, r *http.Request) {
		_, err := ioutil.ReadAll(r.Body)
		assert.NoError(t, err)
		w.WriteHeader(http.StatusOK)
		atomic.AddInt64(&callCount, 1)
	}

	cluster1Urls := mkDummySrvsWithfun(2, t, fReadAllOk)
	conf := configure(cluster1Urls)
	cluster2Urls := mkDummySrvsWithfun(2, t, f10BErr)
	conf.Clusters["test"] = config.ClusterConfig{
		Weight:   1,
		Type:     "replicator",
		Backends: cluster2Urls,
	}

	conf.Client.Clusters = append(conf.Client.Clusters, "test")
	ringFactory := makeRingFactory(conf)

	clientRing, err := ringFactory.clientRing(conf.Client)
	require.NoError(t, err)
	body := bytes.NewBuffer([]byte("12345678901234567890"))
	req, _ := http.NewRequest("PUT", "http://example.com/index/a", body)
	resp, err2 := clientRing.RoundTrip(req)
	require.NoError(t, err2)
	assert.Equal(t, http.StatusOK, resp.StatusCode, "Should handle")

}
