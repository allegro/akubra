package storages

import (
	"fmt"
	"net/http"

	"github.com/allegro/akubra/balancing"
	"github.com/allegro/akubra/log"

	set "github.com/deckarep/golang-set"
)

// NamedShardClient interface
type NamedShardClient interface {
	http.RoundTripper
	Name() string
	Backends() []*StorageClient
}

// ShardClient stores information about cluster backends
type ShardClient struct {
	backends          []*StorageClient
	name              string
	synclog           *SyncSender
	MethodSet         set.Set
	requestDispatcher dispatcher
	balancer          *balancing.BalancerPrioritySet
}

// RoundTrip implements http.RoundTripper interface
func (c *ShardClient) RoundTrip(req *http.Request) (*http.Response, error) {

	reqID, _ := req.Context().Value(log.ContextreqIDKey).(string)
	log.Debugf("Shard: Got request id %s", reqID)
	if c.balancer != nil && (req.Method == http.MethodGet || req.Method == http.MethodHead || req.Method == http.MethodOptions) {
		return c.balancerRoundTrip(req)
	}
	log.Debug("It went through request dispatcher")
	return c.requestDispatcher.Dispatch(req)
}

func (c *ShardClient) balancerRoundTrip(req *http.Request) (resp *http.Response, err error) {
	notFoundNodes := []balancing.Node{}

	for node := c.balancer.GetMostAvailable(notFoundNodes...); node != nil; {
		if node == nil {
			return nil, fmt.Errorf("no avialable node")
		}
		resp, err = node.RoundTrip(req)
		if resp.StatusCode == http.StatusNotFound {
			notFoundNodes = append(notFoundNodes, node)
			continue
		}
		return resp, err
	}
	return resp, err
}

// Name get Cluster name
func (c *ShardClient) Name() string {
	return c.name
}

// TODO: rename to storages

// Backends get http.RoundTripper slice
func (c *ShardClient) Backends() []*StorageClient {
	return c.backends
}

func newShard(name string, storageNames []string, storages map[string]*StorageClient, synclog *SyncSender) (*ShardClient, error) {
	shardStorages := make([]*StorageClient, 0)
	for _, storageName := range storageNames {
		backendRT, ok := storages[storageName]
		if !ok {
			return nil, fmt.Errorf("no such storage %q in 'storages::newShard'", storageName)
		}
		shardStorages = append(shardStorages, backendRT)
	}
	log.Debugf("Shard %s storages %v", name, shardStorages)
	cluster := &ShardClient{backends: shardStorages, name: name, requestDispatcher: NewRequestDispatcher(shardStorages, synclog), synclog: synclog}
	return cluster, nil
}
