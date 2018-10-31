package regions

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/allegro/akubra/log"
	"github.com/allegro/akubra/utils"
	"github.com/bnogas/minio-go/pkg/s3signer"

	"net"

	"fmt"
	"github.com/allegro/akubra/regions/config"
	"github.com/allegro/akubra/sharding"
	storage "github.com/allegro/akubra/storages"
)

const defaultRootS3EndpointContent = "<?xml version='1.0' encoding='UTF-8'?>" +
	"<ListAllMyBucketsResult xmlns='http://s3.amazonaws.com/doc/2006-03-01/'>" +
	"<Owner><ID>anonymous</ID><DisplayName></DisplayName></Owner><Buckets></Buckets></ListAllMyBucketsResult>"

// Regions container for multiclusters
type Regions struct {
	multiCluters map[string]sharding.ShardsRingAPI
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
	log.Debugf("Looking for region with domain %s", reqHost)
	if err != nil {
		reqHost = req.Host
	}
	shardsRing, ok := rg.multiCluters[reqHost]
	if ok {
		log.Debugf("Path style request with domain '%s'", reqHost)
		req.Header.Add(utils.InternalPathStyleFlag, "y")
		req.Header.Add(s3signer.CustomStorageHost, reqHost)
		return shardsRing.DoRequest(req)
	}
	host, bucketName := rg.findHostInDomainStyle(reqHost)
	if host != "" && bucketName != "" {
		shardsRing, _ = rg.multiCluters[host]
		req.Header.Add(s3signer.CustomStorageHost, host)
		req.Header.Add(utils.InternalBucketHeader, bucketName)
		log.Debugf("Domain style request with domain '%s' and bucket '%s'", host, bucketName)
		return shardsRing.DoRequest(req)
	} else if bucketName == "" {
		if req.URL.Path == "/" {
			log.Debug("Domain style request with domain '%s' and path='/'", host)
			return rg.healthCheckResolver(req)
		}
	}
	return rg.getNoSuchDomainResponse(req), nil
}

func (rg Regions) findHostInDomainStyle(originalHost string) (currentHost, bucket string) {
	currentHost = ""
	lastSubDomainIndex := strings.LastIndex(originalHost, ".")
	for lastSubDomainIndex != -1 {
		currentHost = originalHost[lastSubDomainIndex+1:] + currentHost
		_, domainFoundInConfig := rg.multiCluters[currentHost]
		if domainFoundInConfig {
			return currentHost, originalHost[:lastSubDomainIndex]
		}
		currentHost = "." + currentHost
		originalHost = originalHost[:lastSubDomainIndex]
		lastSubDomainIndex = strings.LastIndex(originalHost, ".")
	}
	return
}

func (rg Regions) healthCheckResolver(req *http.Request) (resp *http.Response, err error) {
	resp = &http.Response{
		Status:        fmt.Sprintf("%d OK", http.StatusOK),
		StatusCode:    http.StatusOK,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Body:          ioutil.NopCloser(bytes.NewBufferString(defaultRootS3EndpointContent)),
		ContentLength: int64(len(defaultRootS3EndpointContent)),
		Request:       req,
		Header:        make(http.Header, 0),
	}

	return resp, nil
}

// NewRegions build new region http.RoundTripper
func NewRegions(conf config.Regions, storages storage.Storages, syncLogger log.Logger) (http.RoundTripper, error) {

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
	}
	return regions, nil
}
