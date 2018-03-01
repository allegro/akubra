package storages

import (
	"net/http"
	"testing"

	"net/url"

	config "github.com/allegro/akubra/storages/config"
	"github.com/allegro/akubra/transport"
	"github.com/allegro/akubra/types"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type StorageTestSuite struct {
	suite.Suite
	storage  Storages
	cluster1 *Cluster
	cluster2 *Cluster
}

func (suite *StorageTestSuite) SetupTest() {
	suite.storage = Storages{Clusters: make(map[string]NamedCluster)}
	suite.cluster1 = &Cluster{
		name:     "test1",
		backends: []http.RoundTripper{http.DefaultTransport},
	}
	suite.storage.Clusters["test1"] = suite.cluster1
	suite.cluster2 = &Cluster{
		name:     "test2",
		backends: []http.RoundTripper{http.DefaultTransport},
	}
}

func (suite *StorageTestSuite) TestGetClusterShouldReturnDefinedCluster() {
	c, err := suite.storage.GetCluster(suite.cluster1.Name())

	require.NoError(suite.T(), err)
	require.Equal(suite.T(), suite.cluster1, c)
}

func (suite *StorageTestSuite) TestGetClusterShouldReturnErrorIfClusterIsNotDefined() {
	c, err := suite.storage.GetCluster("notExists")
	require.Equal(suite.T(), &Cluster{}, c)
	require.Error(suite.T(), err)
}

func (suite *StorageTestSuite) TestClusterShardsShouldReturnClusterOfGivenNameIfItsAlreadyDefined() {
	rCluster := suite.storage.ClusterShards("test1", suite.cluster2)

	require.Equal(suite.T(), suite.cluster1, rCluster)
}

func (suite *StorageTestSuite) TestClusterShardsShouldReturnJoinedCluster() {
	rCluster := suite.storage.ClusterShards("test", suite.cluster1, suite.cluster2)

	require.Equal(suite.T(), "test", rCluster.Name())
	require.Contains(suite.T(), rCluster.Backends(), suite.cluster1.Backends()[0])
	require.Contains(suite.T(), rCluster.Backends(), suite.cluster2.Backends()[0])

	require.Len(suite.T(), rCluster.Backends(), 2)
	require.Equal(suite.T(), suite.storage.Clusters["test"], rCluster)
}

func TestStorageTestSuite(t *testing.T) {
	suite.Run(t, new(StorageTestSuite))
}

func TestShouldNotInitStoragesWithWrongBackendType(t *testing.T) {
	backendName := "backend1"
	backendType := "unknown"
	var transportContainer transport.Container
	clustersConf := config.ClustersMap{}
	clusterConfig := config.Cluster{
		Backends: []string{"http://localhost"},
	}
	clustersConf["clusterName1"] = clusterConfig

	urlBackend := url.URL{Scheme: "http", Host: "localhost"}
	backendsConf := config.BackendsMap{backendName: config.Backend{
		Endpoint:    types.YAMLUrl{URL: &urlBackend},
		Maintenance: false,
		Properties:  nil,
		Type:        backendType,
	}}
	var respHandler transport.MultipleResponsesHandler

	_, err := InitStorages(transportContainer, clustersConf, backendsConf, respHandler)

	require.Error(t, err)
	require.Contains(t, err.Error(),
		"initialization of backend 'backend1' resulted with error: no decorator defined for type 'unknown'")
}
