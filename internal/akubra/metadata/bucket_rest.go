package metadata

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	akubraHttp "github.com/allegro/akubra/internal/akubra/http"
	"github.com/allegro/akubra/internal/akubra/log"
)

const (
	restEndpointPattern     = "%s/%s"
	responseStatusErrFormat = "unexpected response status %d"
	unmarshallError         = "failed to unmarshall response"
	internal                = "INTERNAL"
)

//BucketIndexRestService is an implementation of BucketMetaDataFetcher that talks to a rest service
//assuming it's supporint the requried protocol, meaning `/bucket/{bucketName}` returns the bucket metadta
type BucketIndexRestService struct {
	httpClient akubraHttp.Client
	endpoint   string
}

type bucketMataDataJSON struct {
	BucketName string `json:"name"`
	Visibility string `json:"visibility"`
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
		return nil, &FetchError{
			Message: bucketLocation.Name,
			Code:    NotFound,
		}
	}
	if httpResponse.StatusCode != http.StatusOK || httpResponse.Body == nil {
		return nil, &FetchError{
			Message: fmt.Sprintf(responseStatusErrFormat, httpResponse.StatusCode),
			Code:    BadResponse,
		}
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
		return nil, &FetchError{
			Message:     unmarshallError,
			Code:        BadResponse,
			ParentError: err}
	}
	if err := reader.Close(); err != nil {
		log.Debugf("failed to close reader: %q", err)
	}

	var metaData bucketMataDataJSON
	err = json.Unmarshal(bytes, &metaData)
	if err != nil {
		return nil, &FetchError{
			Message:     unmarshallError,
			Code:        BadResponse,
			ParentError: err}
	}

	return &metaData, nil
}
