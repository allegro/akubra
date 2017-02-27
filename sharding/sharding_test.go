package sharding

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/allegro/akubra/config"
	"github.com/allegro/akubra/httphandler"
	"github.com/allegro/akubra/log"

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

	syncLogger := log.DefaultLogger
	accessLogger := log.DefaultLogger
	mainLogger := log.DefaultLogger
	clsyncLogger := log.DefaultLogger

	defaultClusterConfig.Backends = backends

	clustersConf := make(map[string]config.ClusterConfig)
	clustersConf["cluster1"] = defaultClusterConfig

	clientCfg := &config.ClientConfig{
		Name:     "client1",
		Clusters: []string{"cluster1"},
	}

	return config.Config{
		YamlConfig: config.YamlConfig{
			ConnLimit:             connLimit,
			ConnectionTimeout:     timeout,
			ConnectionDialTimeout: timeout,
			Client:                clientCfg,
			Clusters:              clustersConf,
		},
		SyncLogMethodsSet: methodsSet,
		Synclog:           syncLogger,
		Accesslog:         accessLogger,
		Mainlog:           mainLogger,
		ClusterSyncLog:    clsyncLogger,
	}
}

func makeRingFactory(conf config.Config) (ringFactory, error) {
	httptransp, err := httphandler.ConfigureHTTPTransport(conf)
	if err != nil {
		return ringFactory{}, err
	}
	if err != nil {
		return ringFactory{}, err
	}
	return newRingFactory(conf, httptransp), nil
}

func TestSingleClusterOnRing(t *testing.T) {
	stream := []byte("cluster1")
	cluster1Urls := mkDummySrvs(2, stream, t)
	conf := configure(cluster1Urls)
	ringFactory, err := makeRingFactory(conf)
	require.NoError(t, err)

	clientRing, err := ringFactory.clientRing(*conf.Client)
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

	ringFactory, err := makeRingFactory(conf)
	require.NoError(t, err)

	clientRing, err := ringFactory.clientRing(*conf.Client)
	require.NoError(t, err)

	reader := bytes.NewBuffer([]byte{})
	URL := "http://example.com/myindex/abcdef"
	req, _ := http.NewRequest("GET", URL, reader)
	resp, err := clientRing.RoundTrip(req)
	require.NoError(t, err)

	respBody := make([]byte, 3)
	_, err = io.ReadFull(resp.Body, respBody)
	require.NoError(t, err, "cannot read response")
	assert.Equal(t, response1, respBody, fmt.Sprintf("Expected %q", response1))

	req2, _ := http.NewRequest("GET", "http://example.com/myindex/aba", reader)
	resp2, err2 := clientRing.RoundTrip(req2)
	require.NoError(t, err2)

	respBody2 := make([]byte, 3)
	_, err = io.ReadFull(resp2.Body, respBody2)
	require.NoError(t, err, "cannot read response")
	assert.Equal(t, response2, respBody2, fmt.Sprintf("Expected %q", response2))
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
	ringFactory, err := makeRingFactory(conf)
	require.NoError(t, err)

	clientRing, err := ringFactory.clientRing(*conf.Client)
	require.NoError(t, err)

	reader := bytes.NewBuffer([]byte{})
	req, _ := http.NewRequest("GET", "http://example.com/index/", reader)
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
	ringFactory, err := makeRingFactory(conf)
	require.NoError(t, err)

	clientRing, err := ringFactory.clientRing(*conf.Client)
	require.NoError(t, err)

	reader := bytes.NewBuffer([]byte{})
	req, _ := http.NewRequest("GET", "http://example.com/index/a", reader)

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
	ringFactory, err := makeRingFactory(conf)
	require.NoError(t, err)

	clientRing, err := ringFactory.clientRing(*conf.Client)
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
	ringFactory, err := makeRingFactory(conf)
	require.NoError(t, err)

	clientRing, err := ringFactory.clientRing(*conf.Client)
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
	ringFactory, err := makeRingFactory(conf)
	require.NoError(t, err)

	clientRing, err := ringFactory.clientRing(*conf.Client)
	require.NoError(t, err)

	body := bytes.NewReader([]byte("12345678901234567890"))
	req, _ := http.NewRequest("POST", "http://example.com/index/a", body)
	resp, err2 := clientRing.RoundTrip(req)
	require.NoError(t, err2)
	assert.Equal(t, http.StatusOK, resp.StatusCode, "Should handle")
	_, errSeek := body.Seek(0, io.SeekStart)
	require.NoError(t, errSeek)
	req, _ = http.NewRequest("PUT", "http://example.com/index/a", body)
	resp, err3 := clientRing.RoundTrip(req)
	require.NoError(t, err3)
	assert.Equal(t, http.StatusTeapot, resp.StatusCode, "Should return err if PUT fails on first cluster")
}
