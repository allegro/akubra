package sharding

//
//import (
//	"net/http"
//	"net/http/httptest"
//	"net/url"
//	"testing"
//
//	"sync/atomic"
//
//	"github.com/allegro/akubra/config"
//	"github.com/allegro/akubra/httphandler"
//	httphandlerconfig "github.com/allegro/akubra/httphandler/config"
//	"github.com/allegro/akubra/log"
//	regionsconfig "github.com/allegro/akubra/regions/config"
//	//shardingconfig "github.com/allegro/akubra/sharding/config"
//	"fmt"
//
//	"github.com/allegro/akubra/storages"
//	storagesconfig "github.com/allegro/akubra/storages/config"
//	"github.com/allegro/akubra/types"
//	set "github.com/deckarep/golang-set"
//	"github.com/stretchr/testify/assert"
//)
//
//func makePrimaryConfiguration() config.Config {
//	methodsSlice := []string{"PUT", "GET", "DELETE"}
//
//	methodsSet := set.NewThreadUnsafeSet()
//	for _, method := range methodsSlice {
//		methodsSet.Add(method)
//	}
//
//	return config.Config{
//		YamlConfig:        config.YamlConfig{},
//		SyncLogMethodsSet: methodsSet,
//	}
//}
//
//func makeRegionRing(clusterWeights []float64, t *testing.T, handlerfunc func(w http.ResponseWriter, r *http.Request)) ShardsRing {
//	config := makePrimaryConfiguration()
//	clusterMap := make(storagesconfig.ClustersMap)
//	//regionClusterList := make([]shardingconfig.MultiClusterConfig, 0, len(clusterWeights))
//	regionClusters := make([]regionsconfig.RegionCluster, 0, len(clusterWeights))
//	for l := 0; l < len(clusterWeights); l++ {
//		clusterName := fmt.Sprintf("cluster%d", l)
//
//		//"Clusters" part...
//		handlerfun := http.HandlerFunc(handlerfunc)
//		ts := httptest.NewServer(handlerfun)
//		backendURL, err := url.Parse(ts.URL)
//		if err != nil {
//			t.Error(err)
//		}
//		backendYamlURL := &types.YAMLUrl{URL: backendURL}
//		backends := []string{backendYamlURL.Host}
//		clusterConfig := storagesconfig.Cluster{
//			Backends: backends,
//		}
//		clusterMap[clusterName] = clusterConfig
//
//		//"Regions" part...
//		//multiClusterConfig := &shardingconfig.MultiClusterConfig{
//		//	Cluster: clusterName,
//		//	Weight:  clusterWeights[l],
//		//}
//		//regionClusterList = append(regionClusterList, *multiClusterConfig)
//		rc := regionsconfig.RegionCluster{
//			Name:   clusterName,
//			Weight: clusterWeights[l],
//		}
//		regionClusters = append(regionClusters, rc)
//	}
//
//	domains := []string{"http://regiondomain.pl"}
//	regionConfig := &regionsconfig.Region{
//		Clusters: regionClusters,
//		Domains:  domains,
//		Default:  true,
//	}
//	config.Clusters = clusterMap
//
//	httptransp, err := httphandler.ConfigureHTTPTransport(httphandlerconfig.Client{})
//	if err != nil {
//		t.Error(err)
//	}
//	ringStorages := storages.Storages{
//		Clusters: make(map[string]storages.Cluster),
//		Backends: make(map[string]http.RoundTripper),
//	}
//
//	regions := regionsconfig.Regions{}
//	syncLogger := log.DefaultLogger
//
//	ringFactory := NewRingFactory(regions, ringStorages, httptransp, syncLogger)
//	regionRing, err := ringFactory.RegionRing("regionName", *regionConfig)
//	if err != nil {
//		t.Error(err)
//	}
//	return regionRing
//}
//
//func TestGetWithOneCluster(t *testing.T) {
//	callCount := int32(0)
//	f := func(w http.ResponseWriter, r *http.Request) {
//		w.WriteHeader(http.StatusOK)
//		atomic.AddInt32(&callCount, 1)
//	}
//	regionRing := makeRegionRing([]float64{1}, t, f)
//	reqURL, _ := url.Parse("http://allegro.pl/b/o")
//	request := &http.Request{
//		URL:    reqURL,
//		Method: "GET",
//	}
//	response, _ := regionRing.DoRequest(request)
//	assert.Equal(t, int32(1), callCount)
//	assert.Equal(t, http.StatusOK, response.StatusCode)
//}

// func TestGetWithTwoClusters(t *testing.T) {
// 	callCount := int32(0)
// 	f := func(w http.ResponseWriter, r *http.Request) {
// 		w.WriteHeader(http.StatusOK)
// 		atomic.AddInt32(&callCount, 1)
// 	}
// 	regionRing := makeRegionRing([]float64{1, 1}, t, f)
// 	reqURL, _ := url.Parse("http://allegro.pl/b/o")
// 	request := &http.Request{
// 		URL:    reqURL,
// 		Method: "GET",
// 	}
// 	response, _ := regionRing.DoRequest(request)
// 	assert.Equal(t, int32(1), callCount)
// 	assert.Equal(t, http.StatusOK, response.StatusCode)
// }

// func TestGetWithTwoClustersAndRegression(t *testing.T) {
// 	callCount := int32(0)
// 	f := func(w http.ResponseWriter, r *http.Request) {
// 		w.WriteHeader(http.StatusNotFound)
// 		atomic.AddInt32(&callCount, 1)
// 	}
// 	regionRing := makeRegionRing([]float64{0, 1}, t, f)
// 	reqURL, _ := url.Parse("http://allegro.pl/b/o")
// 	request := &http.Request{
// 		URL:    reqURL,
// 		Method: "GET",
// 	}
// 	response, _ := regionRing.DoRequest(request)
// 	assert.Equal(t, int32(2), callCount)
// 	assert.Equal(t, http.StatusNotFound, response.StatusCode)
// }

// func TestDeleteWithTwoClusters(t *testing.T) {
// 	callCount := int32(0)
// 	f := func(w http.ResponseWriter, r *http.Request) {
// 		w.WriteHeader(http.StatusNotFound)
// 		atomic.AddInt32(&callCount, 1)
// 	}
// 	regionRing := makeRegionRing([]float64{1, 1}, t, f)
// 	reqURL, _ := url.Parse("http://allegro.pl/b/o")
// 	request := &http.Request{
// 		URL:    reqURL,
// 		Method: "DELETE",
// 	}
// 	response, _ := regionRing.DoRequest(request)
// 	assert.Equal(t, int32(2), callCount)
// 	assert.Equal(t, http.StatusNotFound, response.StatusCode)
// }

// func TestPutWithTwoClustersAndBucketOnly(t *testing.T) {
// 	callCount := int32(0)
// 	f := func(w http.ResponseWriter, r *http.Request) {
// 		w.WriteHeader(http.StatusOK)
// 		atomic.AddInt32(&callCount, 1)
// 	}
// 	regionRing := makeRegionRing([]float64{1, 1}, t, f)
// 	reqURL, _ := url.Parse("http://allegro.pl/b")
// 	request := &http.Request{
// 		URL:    reqURL,
// 		Method: "PUT",
// 	}
// 	response, _ := regionRing.DoRequest(request)
// 	assert.Equal(t, int32(2), callCount)
// 	assert.Equal(t, http.StatusOK, response.StatusCode)
// }
