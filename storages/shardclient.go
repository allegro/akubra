package storages

import (
	"errors"
	"fmt"
	"github.com/allegro/akubra/watchdog/config"
	"net/http"

	"github.com/allegro/akubra/balancing"
	"github.com/allegro/akubra/log"
	"github.com/allegro/akubra/utils"
	"github.com/allegro/akubra/watchdog"
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
	backends                  []*StorageClient
	name                      string
	MethodSet                 set.Set
	requestDispatcher         dispatcher
	balancer                  *balancing.BalancerPrioritySet
	watchdogVersionHeaderName string
}

// RoundTrip implements http.RoundTripper interface
func (shardClient *ShardClient) RoundTrip(request *http.Request) (*http.Response, error) {
	reqID, _ := request.Context().Value(log.ContextreqIDKey).(string)
	log.Debugf("Shard: Got request id %s", reqID)
	if shardClient.balancer != nil && (request.Method == http.MethodGet || request.Method == http.MethodHead || request.Method == http.MethodOptions) {
		resp, err := shardClient.balancerRoundTrip(request)
		log.Debugf("Request %s, processed by balancer error %s", reqID, err)
		return resp, err

	}
	log.Debugf("Request %s processed by dispatcher", reqID)
	return shardClient.requestDispatcher.Dispatch(request)
}

func (shardClient *ShardClient) balancerRoundTrip(req *http.Request) (resp *http.Response, err error) {
	var notFoundNodes []balancing.Node
	reqID, _ := req.Context().Value(log.ContextreqIDKey).(string)
	log.Printf("Balancer RoundTrip %s", reqID)
	if err != nil {
		return nil, errors.New("regions not configured properly")
	}
	for node := shardClient.balancer.GetMostAvailable(notFoundNodes...); node != nil; node = shardClient.balancer.GetMostAvailable(notFoundNodes...) {
		log.Printf("Balancer roundTrip node loop %s %s", node.Name, reqID)
		if node == nil {
			return nil, fmt.Errorf("no avialable node")
		}
		request, err := utils.ReplicateRequest(req)
		if err != nil {
			return nil, err
		}
		resp, err = node.RoundTrip(request)
		if (resp == nil && err != balancing.ErrNoActiveNodes) || resp.StatusCode == http.StatusNotFound {
			notFoundNodes = append(notFoundNodes, node)
			continue
		}
		if err == nil && len(notFoundNodes) > 0 {
			utils.PutResponseHeaderToContext(resp, shardClient.watchdogVersionHeaderName, req.Context(), string(watchdog.ReadRepairObjectVersion))
		}
		return resp, err
	}
	return resp, err
}

// Name get Cluster name
func (shardClient *ShardClient) Name() string {
	return shardClient.name
}

// TODO: rename to storages

// Backends get http.RoundTripper slice
func (shardClient *ShardClient) Backends() []*StorageClient {
	return shardClient.backends
}

// ShardFactory creates shards
type shardFactory struct {
	watchdog                 watchdog.ConsistencyWatchdog
	consistencyRecordFactory watchdog.ConsistencyRecordFactory
	watchdogConfig           *config.WatchdogConfig
}

func (factory *shardFactory) newShard(name string, storageNames []string, storages map[string]*StorageClient) (*ShardClient, error) {
	shardStorages := make([]*StorageClient, 0)
	for _, storageName := range storageNames {
		backendRT, ok := storages[storageName]
		if !ok {
			return nil, fmt.Errorf("no such storage %q in 'storages::newShard'", storageName)
		}
		shardStorages = append(shardStorages, backendRT)
	}
	log.Debugf("Shard %s storages %v", name, shardStorages)
	requestDispatcher := NewRequestDispatcher(shardStorages)
	return &ShardClient{backends: shardStorages,
		name:                      name,
		requestDispatcher:         requestDispatcher,
		watchdogVersionHeaderName: factory.watchdogConfig.ObjectVersionHeaderName}, nil
}
