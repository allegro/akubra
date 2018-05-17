package storages

import (
	"context"
	"net/http"
	"testing"

	"github.com/allegro/akubra/httphandler"
	"github.com/allegro/akubra/log"

	set "github.com/deckarep/golang-set"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

// ClusterTestSuite setups cluster test suite
type ClusterTestSuite struct {
	suite.Suite
	cluster  Cluster
	backend1 *testRoundTripper
	backend2 *testRoundTripper
}

// TestClusterTestSuite is cluster unit test
func TestClusterTestSuite(t *testing.T) {
	suite.Run(t, new(ClusterTestSuite))
}

// SetupTest conforms suite interface
func (suite *ClusterTestSuite) SetupTest() {
	clusterName := "testCluster"
	backendsNames := []string{"testbackend1", "testbackend2"}

	backendsMap := map[string]http.RoundTripper{}
	for _, name := range backendsNames {
		backendsMap[name] = &testRoundTripper{}
	}
	suite.backend1 = backendsMap["testbackend1"].(*testRoundTripper)
	suite.backend2 = backendsMap["testbackend2"].(*testRoundTripper)

	synclog := log.DefaultLogger

	respHandler := httphandler.LateResponseHandler(synclog, set.NewSet())

	cluster, err := newCluster(
		clusterName,
		backendsNames,
		backendsMap,
		respHandler,
		synclog,
	)

	require := suite.Require()

	require.NoError(err)
	suite.cluster = *cluster
}

// TestSuccessObjectRequest is basic flow
func (suite *ClusterTestSuite) TestSuccessObjectRequest() {
	require := suite.Require()

	cluster := suite.cluster
	request, err := makeGetObjectRequest()
	require.NoError(err)

	okResponse := makeSuccessfulResponse(request, http.StatusOK)

	suite.backend1.On("RoundTrip", request).Return(okResponse, nil)
	suite.backend2.On("RoundTrip", request).Return(okResponse, nil)

	resp, err := cluster.RoundTrip(request)
	require.NoError(err)
	require.NotNil(resp)
	require.Equal(resp.StatusCode, http.StatusOK)
}

func makeGetObjectRequest() (*http.Request, error) {
	request, err := http.NewRequest("GET", "http://not-exist.local/testbucket/testkey", nil)
	if err != nil {
		return request, err
	}
	valueCtx := context.WithValue(context.Background(), log.ContextreqIDKey, "testid")
	req := request.WithContext(valueCtx)
	return req, err
}

func makeSuccessfulResponse(request *http.Request, status int) *http.Response {
	resp := &http.Response{Request: request, StatusCode: status}
	return resp
}

type testRoundTripper struct {
	mock.Mock
}

func (trt *testRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	args := trt.Called(request)
	resp := args.Get(0).(*http.Response)
	return resp, args.Error(1)
}
