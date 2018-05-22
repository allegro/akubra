package regions

import (
	"bytes"
	"io/ioutil"
	"net"
	"net/http"

	"github.com/allegro/akubra/log"

	"github.com/allegro/akubra/regions/config"
	"github.com/allegro/akubra/sharding"
	storage "github.com/allegro/akubra/storages"
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
	shardsRing, ok := rg.multiCluters[reqHost]
	if ok {
		return shardsRing.DoRequest(req)
	}
	if rg.defaultRing != nil {
		return rg.defaultRing.DoRequest(req)
	}
	return rg.getNoSuchDomainResponse(req), nil
}

// NewRegions build new region http.RoundTripper
func NewRegions(conf config.Regions, storages storage.ClusterStorage, syncLogger log.Logger) (http.RoundTripper, error) {

	ringFactory := sharding.NewRingFactory(conf, storages, syncLogger)
	regions := &Regions{
		multiCluters: make(map[string]sharding.ShardsRingAPI),
	}

	for name, regionConfig := range conf {
		regionRing, err := ringFactory.RegionRing(name, regionConfig)
		if err != nil {
			return nil, err
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
