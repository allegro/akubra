package sharding

import (
	"net/http"
	"net/url"
	"testing"

	"fmt"

	"github.com/allegro/akubra/config"
	"github.com/allegro/akubra/httphandler"
	httphandlerconfig "github.com/allegro/akubra/httphandler/config"
	"github.com/allegro/akubra/log"
	regionsconfig "github.com/allegro/akubra/regions/config"

	"github.com/allegro/akubra/storages"
	storagesconfig "github.com/allegro/akubra/storages/config"
	set "github.com/deckarep/golang-set"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type NamedClusterMock struct {
	mock.Mock
}

type RoundTripperMocked struct {
	mock.Mock
}

type NamedCluster = storages.NamedCluster

func (m *NamedClusterMock) RoundTrip(req *http.Request) (*http.Response, error) {
	args := m.MethodCalled("RoundTrip", req)
	response := args.Get(0).(*http.Response)
	err := args.Error(1)
	return response, err
}

func (m *NamedClusterMock) Name() string {
	return "cluster0"
}

func (m *NamedClusterMock) Backends() []http.RoundTripper {
	rt := []http.RoundTripper{}
	return rt
}

type Request = http.Request
type Response = http.Response

func (rtm *RoundTripperMocked) RoundTrip(req *Request) (*Response, error) {
	args := rtm.MethodCalled("RoundTrip", req)
	response := args.Get(0).(*http.Response)
	err := args.Get(1).(error)
	return response, err
}

func makePrimaryConfiguration() config.Config {
	methodsSlice := []string{"PUT", "GET", "DELETE"}

	methodsSet := set.NewThreadUnsafeSet()
	for _, method := range methodsSlice {
		methodsSet.Add(method)
	}

	return config.Config{
		YamlConfig:        config.YamlConfig{},
		SyncLogMethodsSet: methodsSet,
	}
}

func makeRegionRing(clusterWeights []float64, t *testing.T, request *http.Request, httpExpectedStatus int) ShardsRing {
	config := makePrimaryConfiguration()
	clusterMap := make(storagesconfig.ClustersMap)
	regionClusters := make([]regionsconfig.RegionCluster, 0, len(clusterWeights))
	ringStorages := &storages.Storages{}
	ringStorages.Clusters = make(map[string]NamedCluster, len(clusterWeights))

	for l := 0; l < len(clusterWeights); l++ {
		clusterName := fmt.Sprintf("cluster%d", l)

		backends := []string{"http://localhost"}
		clusterConfig := storagesconfig.Cluster{
			Backends: backends,
		}
		clusterMap[clusterName] = clusterConfig

		namedClusterMock := new(NamedClusterMock)
		namedClusterMock.On("Name").Return(clusterName)
		namedClusterMock.On("Backends").Return(map[string]http.RoundTripper{"rt0": &RoundTripperMocked{}})
		resp := &http.Response{StatusCode: httpExpectedStatus}
		var err error
		namedClusterMock.On("RoundTrip", request).Return(resp, err)

		ringStorages.Clusters[clusterName] = namedClusterMock

		rc := regionsconfig.RegionCluster{
			Name:   clusterName,
			Weight: clusterWeights[l],
		}
		regionClusters = append(regionClusters, rc)
	}

	domains := []string{"http://regiondomain.pl"}
	regionConfig := &regionsconfig.Region{
		Clusters: regionClusters,
		Domains:  domains,
		Default:  true,
	}
	config.Clusters = clusterMap

	httptransp, err := httphandler.ConfigureHTTPTransport(httphandlerconfig.Client{})
	if err != nil {
		t.Error(err)
	}

	regions := regionsconfig.Regions{}
	syncLogger := log.DefaultLogger

	ringFactory := NewRingFactory(regions, *ringStorages, httptransp, syncLogger)
	regionRing, err := ringFactory.RegionRing("regionName", *regionConfig)
	if err != nil {
		t.Error(err)
	}
	return regionRing
}

func TestGetWithOneCluster(t *testing.T) {
	expectedStatus := http.StatusOK
	reqURL, _ := url.Parse("http://allegro.pl/b/o")
	request := &http.Request{
		URL:    reqURL,
		Method: "GET",
		Header: http.Header{},
	}
	regionRing := makeRegionRing([]float64{1}, t, request, expectedStatus)
	response, err := regionRing.DoRequest(request)
	assert.Nil(t, err)
	assert.Equal(t, expectedStatus, response.StatusCode)
}

func TestGetWithTwoClusters(t *testing.T) {
	expectedStatus := http.StatusOK
	reqURL, _ := url.Parse("http://allegro.pl/b/o")
	request := &http.Request{
		URL:    reqURL,
		Method: "GET",
		Header: http.Header{},
	}
	regionRing := makeRegionRing([]float64{1, 1}, t, request, expectedStatus)
	response, err := regionRing.DoRequest(request)
	assert.Nil(t, err)
	assert.Equal(t, expectedStatus, response.StatusCode)
}

func TestGetWithTwoClustersAndRegression(t *testing.T) {
	expectedStatus := http.StatusNotFound
	reqURL, _ := url.Parse("http://allegro.pl/b/o")
	request := &http.Request{
		URL:    reqURL,
		Method: "GET",
		Header: http.Header{},
	}
	regionRing := makeRegionRing([]float64{0, 1}, t, request, expectedStatus)
	response, err := regionRing.DoRequest(request)
	assert.Nil(t, err)
	assert.Equal(t, expectedStatus, response.StatusCode)
}

//TODO: mocking
//func TestDeleteWithTwoClusters(t *testing.T) {
//	expectedStatus := http.StatusNotFound
//	reqURL, _ := url.Parse("http://allegro.pl/b/o")
//	request := &http.Request{
//		URL:    reqURL,
//		Method: "DELETE",
//		Header: http.Header{},
//	}
//	regionRing := makeRegionRing([]float64{1, 1}, t, request, expectedStatus)
//	response, err := regionRing.DoRequest(request)
//	assert.Nil(t, err)
//	assert.Equal(t, expectedStatus, response.StatusCode)
//}

func TestPutWithTwoClustersAndBucketOnly(t *testing.T) {
	expectedStatus := http.StatusOK
	reqURL, _ := url.Parse("http://allegro.pl/b/o")
	request := &http.Request{
		URL:    reqURL,
		Method: "PUT",
		Header: http.Header{},
	}
	regionRing := makeRegionRing([]float64{1, 1}, t, request, expectedStatus)
	response, err := regionRing.DoRequest(request)
	assert.Nil(t, err)
	assert.Equal(t, expectedStatus, response.StatusCode)
}
