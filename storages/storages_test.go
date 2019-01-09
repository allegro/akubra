package storages

import (
	"net/http"
	"testing"

	"net/url"

	"github.com/allegro/akubra/storages/config"
	"github.com/allegro/akubra/types"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type StorageTestSuite struct {
	suite.Suite
	storage  Storages
	cluster1 *ShardClient
	cluster2 *ShardClient
}

func (suite *StorageTestSuite) SetupTest() {
	suite.storage = Storages{ShardClients: make(map[string]NamedShardClient)}
	suite.cluster1 = &ShardClient{
		name:     "test1",
		backends: []*StorageClient{&StorageClient{RoundTripper: http.DefaultTransport}},
	}
	suite.storage.ShardClients["test1"] = suite.cluster1
	suite.cluster2 = &ShardClient{
		name:     "test2",
		backends: []*StorageClient{&StorageClient{RoundTripper: http.DefaultTransport}},
	}
}

func (suite *StorageTestSuite) TestGetClusterShouldReturnDefinedCluster() {
	c, err := suite.storage.GetShard(suite.cluster1.Name())

	require.NoError(suite.T(), err)
	require.Equal(suite.T(), suite.cluster1, c)
}

func (suite *StorageTestSuite) TestGetClusterShouldReturnErrorIfClusterIsNotDefined() {
	c, err := suite.storage.GetShard("notExists")
	require.Equal(suite.T(), &ShardClient{}, c)
	require.Error(suite.T(), err)
}

func TestStorageTestSuite(t *testing.T) {
	suite.Run(t, new(StorageTestSuite))
}

func TestShouldNotInitStoragesWithWrongBackendType(t *testing.T) {
	backendName := "backend1"
	backendType := "unknown"
	clustersConf := config.ShardsMap{}
	clusterConfig := config.Shard{
		Storages: config.Storages{{Name: "http://localhost"}},
	}
	clustersConf["clusterName1"] = clusterConfig

	urlBackend := url.URL{Scheme: "http", Host: "localhost"}
	storagesMap := config.StoragesMap{backendName: config.Storage{
		Backend:     types.YAMLUrl{URL: &urlBackend},
		Maintenance: false,
		Properties:  nil,
		Type:        backendType,
	}}

	_, err := InitStorages(http.DefaultTransport, clustersConf, storagesMap, nil)

	require.Error(t, err)
	require.Contains(t, err.Error(),
		"initialization of backend 'backend1' resulted with error: no decorator defined for type 'unknown'")
}
