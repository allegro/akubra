package storages

import (
	"net/http"
	"testing"

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

func (suite *StorageTestSuite) TestJoinClustersShouldReturnClusterOfGivenNameIfItsAlreadyDefined() {
	rCluster := suite.storage.JoinClusters("test1", suite.cluster2)

	require.Equal(suite.T(), suite.cluster1, rCluster)
}

func (suite *StorageTestSuite) TestJoinClustersShouldReturnJoinedCluster() {
	rCluster := suite.storage.JoinClusters("test", suite.cluster1, suite.cluster2)

	require.Equal(suite.T(), "test", rCluster.Name())
	require.Contains(suite.T(), rCluster.Backends(), suite.cluster1.Backends()[0])
	require.Contains(suite.T(), rCluster.Backends(), suite.cluster2.Backends()[0])

	require.Len(suite.T(), rCluster.Backends(), 2)
	require.Equal(suite.T(), suite.storage.Clusters["test"], rCluster)
}

func TestStorageTestSuite(t *testing.T) {
	suite.Run(t, new(StorageTestSuite))
}
