package storages

import (
	"errors"
	"fmt"
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
	backends            []*StorageClient
	name                string
	MethodSet           set.Set
	requestDispatcher   dispatcher
	balancer            *balancing.BalancerPrioritySet
	consistencyWatchdog watchdog.ConsistencyWatchdog
	recordFactory       watchdog.ConsistencyRecordFactory
}

// RoundTrip implements http.RoundTripper interface
func (shardClient *ShardClient) RoundTrip(req *http.Request) (*http.Response, error) {
	request := &Request{
		Request:                          req,
		isMultiPartUploadRequest:         utils.IsMultiPartUploadRequest(req),
		isInitiateMultipartUploadRequest: utils.IsInitiateMultiPartUploadRequest(req),
	}
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

func (shardClient *ShardClient) balancerRoundTrip(req *Request) (resp *http.Response, err error) {
	var notFoundNodes []balancing.Node
	reqID, _ := req.Context().Value(log.ContextreqIDKey).(string)
	log.Printf("Balancer RoundTrip %s", reqID)
	_, isReadRepairOn, err := extractRegionPropsFrom(req)
	if err != nil {
		return nil, errors.New("regions not configured properly")
	}
	for node := shardClient.balancer.GetMostAvailable(notFoundNodes...); node != nil; node = shardClient.balancer.GetMostAvailable(notFoundNodes...) {
		log.Printf("Balancer roundTrip node loop %s %s", node.Name, reqID)
		if node == nil {
			return nil, fmt.Errorf("no avialable node")
		}
		resp, err = node.RoundTrip(req.Request)
		if (resp == nil && err != balancing.ErrNoActiveNodes) || resp.StatusCode == http.StatusNotFound {
			notFoundNodes = append(notFoundNodes, node)
			continue
		}
		if err == nil && isReadRepairOn && len(notFoundNodes) > 0 {
			shardClient.performReadRepair(req.Request, resp)
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
	watchdogConfig           *watchdog.Config
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
	requestDispatcher := NewRequestDispatcher(shardStorages, factory.watchdog, factory.consistencyRecordFactory)
	return &ShardClient{backends: shardStorages,
		name: name,
		requestDispatcher: requestDispatcher,
		consistencyWatchdog: factory.watchdog,
		recordFactory: factory.consistencyRecordFactory}, nil
}

func (shardClient *ShardClient) performReadRepair(request *http.Request, response *http.Response) {
	if response.StatusCode != http.StatusOK {
		return
	}
	currentVersion := response.Header.Get(shardClient.consistencyWatchdog.GetVersionHeaderName())
	if currentVersion == "" {
		log.Debugf("Can't perform read repair, no version header found, reqID %s", request.Context().Value(log.ContextreqIDKey))
		return
	}
	record, err := shardClient.recordFactory.CreateRecordFor(request)
	if err != nil {
		log.Debugf("Failed to perform read repair, couldn't create log record, reqID %s : %s", request.Context().Value(log.ContextreqIDKey), err)
		return
	}
	record.ObjectVersion = currentVersion
	_, err = shardClient.consistencyWatchdog.Insert(record)
	if err != nil {
		log.Debugf("Failed to perform read repair for object %s in domain %s: %s", record.ObjectID, record.Domain, err)
	}
	log.Debugf("Performed read repair for object %s in domain %s: %s", record.ObjectID, record.Domain, err)
}
