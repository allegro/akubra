package regions

import (
	"bytes"
	"context"
	"github.com/allegro/akubra/internal/akubra/utils"
	"io"
	"io/ioutil"
	"net"
	"net/http"

	"github.com/allegro/akubra/internal/akubra/watchdog"

	"github.com/allegro/akubra/internal/akubra/config"
	"github.com/allegro/akubra/internal/akubra/sharding"
	storage "github.com/allegro/akubra/internal/akubra/storages"
)

// Regions container for multiclusters
type Regions struct {
	multiCluters map[string]sharding.ShardsRingAPI
	defaultRing  sharding.ShardsRingAPI
}

func (rg Regions) assignShardsRing(domain string, shardRing sharding.ShardsRingAPI) {
	rg.multiCluters[domain] = shardRing
}

func (rg Regions) getNoSuchDomainResponse(req *http.Request) *http.Response {
	body := "No region found for this domain."
	return &http.Response{
		Status:        "404 Not found",
		StatusCode:    404,
		Proto:         req.Proto,
		Body:          ioutil.NopCloser(bytes.NewBufferString(body)),
		ContentLength: int64(len(body)),
		Request:       req,
		Header:        make(http.Header),
	}
}

// RoundTrip performs round trip to target
func (rg Regions) RoundTrip(req *http.Request) (*http.Response, error) {
	reqHost, _, err := net.SplitHostPort(req.Host)
	if err != nil {
		reqHost = req.Host
	}
	shardsRing := rg.defaultRing
	req, err = prepareRequestBody(req)
	if err != nil {
		return nil, err
	}
	if ringForRequest, foundRingForRequest := rg.multiCluters[reqHost]; foundRingForRequest {
		shardsRing = ringForRequest
	}
	if shardsRing == nil {
		return rg.getNoSuchDomainResponse(req), nil
	}
	req = req.WithContext(shardingPolicyContext(req, reqHost, shardsRing.GetRingProps()))
	return shardsRing.DoRequest(req)
}

func prepareRequestBody(request *http.Request) (*http.Request, error) {
	if request.Body == nil {
		return request, nil
	}
	bodyBytes, err := utils.ReadRequestBody(request)
	if err != nil {
		return nil, err
	}
	request.Body = ioutil.NopCloser(bytes.NewBuffer(bodyBytes))
	request.GetBody = func() (closer io.ReadCloser, e error) {
		return ioutil.NopCloser(bytes.NewBuffer(bodyBytes)), nil
	}
	return request, nil
}

func shardingPolicyContext(request *http.Request, reqHost string, shardProps *sharding.RingProps) context.Context {
	noErrorsDuringRequest := true
	readRepairObjectVersion := ""
	successfulMultipart := false
	shardingContext := context.WithValue(request.Context(), watchdog.Domain, reqHost)
	shardingContext = context.WithValue(shardingContext, watchdog.ConsistencyLevel, shardProps.ConsistencyLevel)
	shardingContext = context.WithValue(shardingContext, watchdog.NoErrorsDuringRequest, &noErrorsDuringRequest)
	shardingContext = context.WithValue(shardingContext, watchdog.ReadRepairObjectVersion, &readRepairObjectVersion)
	shardingContext = context.WithValue(shardingContext, watchdog.MultiPartUpload, &successfulMultipart)
	return context.WithValue(shardingContext, watchdog.ReadRepair, shardProps.ReadRepair)
}

// NewRegions build new region http.RoundTripper
func NewRegions(conf config.Config,
	storages storage.ClusterStorage,
	consistencyWatchdog watchdog.ConsistencyWatchdog,
	recordFactory watchdog.ConsistencyRecordFactory,
	watchdogVersionHeader string) (http.RoundTripper, error) {

	ringFactory := sharding.NewRingFactory(conf, storages)
	regions := &Regions{
		multiCluters: make(map[string]sharding.ShardsRingAPI),
	}
	for name, regionConfig := range conf.ShardingPolicies {
		regionRing, err := ringFactory.RegionRing(name, conf, regionConfig)
		if err != nil {
			return nil, err
		}

		if consistencyWatchdog != nil {
			regionRing = sharding.NewShardingAPI(regionRing, consistencyWatchdog, recordFactory, watchdogVersionHeader)
		}

		for _, domain := range regionConfig.Domains {
			regions.assignShardsRing(domain, regionRing)
		}
		if regionConfig.Default {
			regions.defaultRing = regionRing
		}
	}

	return regions, nil
}