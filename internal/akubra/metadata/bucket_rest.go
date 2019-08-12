package metadata

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	akubraHttp "github.com/allegro/akubra/internal/akubra/http"
	"github.com/allegro/akubra/internal/akubra/log"
)

const (
	restEndpointPattern     = "%s/bucket/%s"
	responseStatusErrFormat = "unexpected response status %d"
	internal                = "INTERNAL"
)

//BucketIndexRestService is an implementation of BucketMetaDataFetcher that talks to a rest service
//assuming it's supporint the requried protocol, meaning `/bucket/{bucketName}` returns the bucket metadta
type BucketIndexRestService struct {
	httpClient akubraHttp.Client
	endpoint   string
}

//BucketIndexRestServiceFactory creates instances of BucketIndexRestService
type BucketIndexRestServiceFactory struct {
	httpClient akubraHttp.Client
}

//NewBucketIndexRestServiceFactory creates an instance of BucketIndexRestServiceFactory
func NewBucketIndexRestServiceFactory(httpClient akubraHttp.Client) BucketMetaDataFetcherFactory {
	return &BucketIndexRestServiceFactory{httpClient: httpClient}
}

//Create creates an instance of FakeBucketMetaDataFetcher
func (factory *BucketIndexRestServiceFactory) Create(config map[string]string) (BucketMetaDataFetcher, error) {
	httpEndpoint, present := config["HTTPEndpoint"]
	if !present {
		return nil, errors.New("failed to create BucketIndexRestServiceFactory, 'HTTPEndpoint' missing")
	}
	return &BucketIndexRestService{
		httpClient: factory.httpClient,
		endpoint:   httpEndpoint}, nil
}

type bucketMataDataJSON struct {
	BucketName string `json:"name"`
	Visibility string `json:"visibility"`
}

//NewBucketIndexRestService creates an instance of BucketIndexRestService
func NewBucketIndexRestService(httpClient akubraHttp.Client, endpoint string) BucketMetaDataFetcher {
	return &BucketIndexRestService{
		httpClient: httpClient,
		endpoint:   endpoint,
	}
}

//Fetch fetches the bucket metadata via rest API
func (service *BucketIndexRestService) Fetch(bucketLocation *BucketLocation) (*BucketMetaData, error) {
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
	return &BucketMetaData{
		Name:       metaDataJSON.BucketName,
		IsInternal: metaDataJSON.Visibility == internal}, nil
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
