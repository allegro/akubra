package metadata

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/allegro/akubra/internal/akubra/balancing"
	"github.com/allegro/akubra/internal/akubra/metrics"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/allegro/akubra/internal/akubra/discovery"
	akubraHttp "github.com/allegro/akubra/internal/akubra/http"
	"github.com/allegro/akubra/internal/akubra/log"
)

const (
	restEndpointPattern     = "%s/buckets/%s"
	responseStatusErrFormat = "unexpected response status %d"
	internal                = "internal"
)

//BucketIndexRestService is an implementation of BucketMetaDataFetcher that talks to a rest service
//assuming it's supporint the requried protocol, meaning `/bucket/{bucketName}` returns the bucket metadta
type BucketIndexRestService struct {
	httpClient akubraHttp.Client
	endpoint   string
	breaker balancing.Breaker
}

//BucketIndexRestServiceFactory creates instances of BucketIndexRestService
type BucketIndexRestServiceFactory struct {
	discoveryClient discovery.Client
}

//NewBucketIndexRestServiceFactory creates an instance of BucketIndexRestServiceFactory
func NewBucketIndexRestServiceFactory(discoveryClient discovery.Client) BucketMetaDataFetcherFactory {
	return &BucketIndexRestServiceFactory{discoveryClient: discoveryClient}
}

//Create creates an instance of FakeBucketMetaDataFetcher
func (factory *BucketIndexRestServiceFactory) Create(config map[string]string) (BucketMetaDataFetcher, error) {
	httpEndpoint, present := config["HTTPEndpoint"]
	if !present {
		return nil, errors.New("failed to create BucketIndexRestServiceFactory, 'HTTPEndpoint' missing")
	}
	httpTimeout, present := config["HTTPTimeout"]
	if !present {
		return nil, errors.New("failed to create BucketIndexRestServiceFactory, 'HTTPTimeout' missing")
	}
	timeout, err := time.ParseDuration(httpTimeout)
	if err != nil {
		return nil, errors.New("failed to create BucketIndexRestServiceFactory, 'HTTPTimeout' not parsable")
	}
	httpCli := &http.Client{Timeout: timeout}
	akubraHTTPCli := akubraHttp.NewDiscoveryHTTPClient(factory.discoveryClient, httpCli)
	breaker, err := createBreaker(config)
	if err != nil {
		return nil, fmt.Errorf("SBI breaker not defined properly %w", err)
	}
	return &BucketIndexRestService{
		httpClient: akubraHTTPCli,
		endpoint:   httpEndpoint,
		breaker: breaker}, nil
}
func createBreaker(config map[string]string) (balancing.Breaker, error) {
	callTimeLimit, err := time.ParseDuration(config["BreakerCallTimeLimit"])
	if err != nil {
		return nil, fmt.Errorf("duration parse fail fo field BreakerCallTimeLimit")
	}
	cutOutDuration, err := time.ParseDuration(config["BreakerBasicCutOutDuration"])
	if err != nil {
		return nil, fmt.Errorf("duration parse fail fo field BreakerBasicCutOutDuration")
	}
	maxCutOutDuration, err  := time.ParseDuration(config["BreakerMaxCutOutDuration"])
	if err != nil {
		return nil, fmt.Errorf("duration parse fail fo field BreakerMaxCutOutDuration")
	}
	probeSize, err := strconv.ParseInt(config["BreakerProbeSize"], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("duration parse fail fo field BreakerProbeSize")
	}
	callTimePercentile, err := strconv.ParseFloat(config["BreakerCallTimeLimitPercentile"], 642)
	if err != nil {
		return nil, fmt.Errorf("duration parse fail fo field BreakerCallTimeLimitPercentile")
	}
	errorRate, err := strconv.ParseFloat(config["BreakerErrorRate"], 642)
	if err != nil {
		return nil, fmt.Errorf("duration parse fail fo field BreakerErrorRate")
	}
	return balancing.NewBreaker(int(probeSize),
		callTimeLimit,
		callTimePercentile,
		errorRate,
		cutOutDuration,
		maxCutOutDuration,
	), nil
}
type bucketMataDataJSON struct {
	BucketName string `json:"bucketName"`
	Visibility string `json:"bucketVisibility"`
}

//NewBucketIndexRestService creates an instance of BucketIndexRestService
func NewBucketIndexRestService(httpClient akubraHttp.Client, endpoint string) BucketMetaDataFetcher {
	return &BucketIndexRestService{
		httpClient: httpClient,
		endpoint:   endpoint,
		breaker: nil,
	}
}

//Fetch fetches the bucket metadata via rest API
func (service *BucketIndexRestService) Fetch(bucketLocation *BucketLocation) (*BucketMetaData, error) {
	if service.breaker != nil {
		if service.breaker.ShouldOpen() {
			metrics.UpdateGauge("metadata.bucket.service.broke", 1)
			return nil, fmt.Errorf("bucketIndexRestService.Fetch error: breaker is open")
		}
	}
	bucketMetaDataRequest, err := service.createBucketMetaDataRequest(bucketLocation)
	if err != nil {
		return nil, err
	}
	httpResponse, err := service.httpClient.Do(bucketMetaDataRequest)
	if err != nil {
		return nil, err
	}
	if httpResponse.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if httpResponse.StatusCode != http.StatusOK || httpResponse.Body == nil {
		return nil, fmt.Errorf(responseStatusErrFormat, httpResponse.StatusCode)
	}
	metaDataJSON, err := unmarshallMetaData(httpResponse.Body)

	if err != nil {
		return nil, err
	}

	log.Debugf("Fetched info for bucket %s, visibility: %s", bucketLocation.Name, metaDataJSON.Visibility)

	return &BucketMetaData{
		Name:       metaDataJSON.BucketName,
		IsInternal: strings.ToLower(metaDataJSON.Visibility) == internal}, nil
}

func (service *BucketIndexRestService) createBucketMetaDataRequest(bucketLocation *BucketLocation) (*http.Request, error) {
	bucketMetaDataURL := fmt.Sprintf(
		restEndpointPattern,
		service.endpoint,
		bucketLocation.Name)

	return http.NewRequest(http.MethodGet, bucketMetaDataURL, nil)
}

func unmarshallMetaData(reader io.ReadCloser) (*bucketMataDataJSON, error) {
	bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	if err := reader.Close(); err != nil {
		log.Debugf("failed to close reader: %q", err)
	}
	var metaData bucketMataDataJSON
	err = json.Unmarshal(bytes, &metaData)
	if err != nil {
		return nil, err
	}
	return &metaData, nil
}
