package regions

import (
	"net/http"
	"github.com/allegro/akubra/config"
	"github.com/allegro/akubra/httphandler"
	"github.com/allegro/akubra/storages"
	"github.com/allegro/akubra/sharding"
	"net"
	"io/ioutil"
	"bytes"
)

type Regions struct {
	multiCluters map[string]sharding.ShardsRingApi
}

func (rg Regions) assignShardsRing(domain string, shardRing sharding.ShardsRingApi) {
	rg.multiCluters[domain] = shardRing
}

func (rg Regions) getNoSuchDomainResponse(req *http.Request) (*http.Response) {
	body := "No region found for this domain."
	return &http.Response{
		Status: "404 Not found",
		StatusCode: 404,
		Proto: req.Proto,
		Body: ioutil.NopCloser(bytes.NewBufferString(body)),
		ContentLength:int64(len(body)),
		Request:req,
		Header: make(http.Header, 0),
		
	}
}

func (rg Regions) RoundTrip(req *http.Request) (*http.Response, error) {
	reqHost, _, err := net.SplitHostPort(req.Host)
	if err != nil {
		reqHost = req.Host
	}
	shardsRing, ok := rg.multiCluters[reqHost]
	if ok {
		return shardsRing.DoRequest(req)
	} else {
		return rg.getNoSuchDomainResponse(req), nil
	}
}

func NewHandler(conf config.Config) (http.Handler, error) {
	httptransp, err := httphandler.ConfigureHTTPTransport(conf)
	if err != nil {
		return nil, err
	}
	allStorages := &storages.Storages{
		Conf: conf,
		Transport: httptransp,
		Clusters:  make(map[string]storages.Cluster),
	}
	ringFactory := sharding.NewRingFactory(conf, allStorages, httptransp)
	regions := &Regions{
		multiCluters: make(map[string]sharding.ShardsRingApi),
	}

	for _, regionConfig := range conf.Regions {
		regionRing, err := ringFactory.RegionRing(regionConfig)
		if err != nil {
			return nil, err
		}
		for _, domain := range regionConfig.Domains {
			regions.assignShardsRing(domain, regionRing)
		}
	}
	roundTripper := httphandler.DecorateRoundTripper(conf, regions)
	return httphandler.NewHandlerWithRoundTripper(roundTripper, conf.BodyMaxSize.SizeInBytes)
}
