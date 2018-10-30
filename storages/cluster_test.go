package storages

import (
	"context"
	"net/http"
	"testing"

	"github.com/allegro/akubra/log"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

// ClusterTestSuite setups cluster test suite
type ClusterTestSuite struct {
	suite.Suite
	cluster    Cluster
	dispatcher dispatcher
}

// TestClusterTestSuite is cluster unit test
func TestClusterTestSuite(t *testing.T) {
	suite.Run(t, new(ClusterTestSuite))
}

// SetupTest conforms suite interface
func (suite *ClusterTestSuite) SetupTest() {
	clusterName := "testCluster"
	cluster, err := newCluster(
		clusterName,
		nil,
		nil,
		nil,
	)
	require := suite.Require()
	require.NoError(err)

	cluster.requestDispatcher = newDispatcherMock()

	suite.dispatcher = cluster.requestDispatcher

	suite.cluster = *cluster
}

// TestSuccessObjectRequest is basic flow
func (suite *ClusterTestSuite) TestSuccessObjectRequest() {
	require := suite.Require()

	cluster := suite.cluster
	request, err := makeGetObjectRequest()
	require.NoError(err)

	okResponse := makeSuccessfulResponse(request, http.StatusOK)
	dispatchMock := suite.dispatcher.(*dispatcherMock)
	dispatchMock.On("Dispatch", request).Return(okResponse, nil)

	resp, err := cluster.RoundTrip(request)
	require.NoError(err)
	require.NotNil(resp)
	require.Equal(resp.StatusCode, http.StatusOK)
}

func makeGetObjectRequest() (*http.Request, error) {
	request, err := http.NewRequest("GET", "/testbucket/testkey", nil)
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

type dispatcherMock struct {
	mock.Mock
}

func (trt *dispatcherMock) Dispatch(request *http.Request) (*http.Response, error) {
	args := trt.Called(request)
	resp := args.Get(0).(*http.Response)
	return resp, args.Error(1)
}
func newDispatcherMock() dispatcher {
	return &dispatcherMock{mock.Mock{}}
}
