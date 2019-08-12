package metadata

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type httpClientMock struct {
	*mock.Mock
}

func TestIndexServiceFailure(t *testing.T) {
	bucketName := "test"

	expectedHTTPRequest, _ := http.NewRequest(
		http.MethodGet,
		fmt.Sprintf("service://mock/bucket/%s", bucketName),
		nil)

	indexServiceErr := errors.New("failure")
	httpClient := httpClientMock{Mock: &mock.Mock{}}
	httpClient.On("Do", expectedHTTPRequest).Return(nil, indexServiceErr)

	indexService := NewBucketIndexRestService(&httpClient, "service://mock")

	bucketLocation := BucketLocation{Name: bucketName}
	metaData, err := indexService.Fetch(&bucketLocation)

	assert.Nil(t, metaData)
	assert.Equal(t, indexServiceErr, err)
}

func TestNotExistingBucketMetaDataFetching(t *testing.T) {
	bucketName := "test"

	expectedHTTPRequest, _ := http.NewRequest(
		http.MethodGet,
		fmt.Sprintf("service://mock/bucket/%s", bucketName),
		nil)

	indexServiceResp := http.Response{
		StatusCode: http.StatusNotFound,
		Request:    expectedHTTPRequest,
	}

	httpClient := httpClientMock{Mock: &mock.Mock{}}
	httpClient.On("Do", expectedHTTPRequest).Return(&indexServiceResp, nil)

	indexService := NewBucketIndexRestService(&httpClient, "service://mock")

	bucketLocation := BucketLocation{Name: bucketName}
	metaData, err := indexService.Fetch(&bucketLocation)

	assert.Nil(t, metaData)
	assert.Nil(t, err)
}

func TestUnexpectedHTTPResponse(t *testing.T) {

	for _, testCase := range []struct {
		statusCode   int
		responseBody io.ReadCloser
	}{
		{statusCode: http.StatusPermanentRedirect, responseBody: nil},
		{statusCode: http.StatusPartialContent, responseBody: ioutil.NopCloser(bytes.NewBuffer([]byte("test")))},
	} {
		bucketName := "test"

		expectedHTTPRequest, _ := http.NewRequest(
			http.MethodGet,
			fmt.Sprintf("service://mock/bucket/%s", bucketName),
			nil)

		indexServiceResp := http.Response{
			StatusCode: testCase.statusCode,
			Request:    expectedHTTPRequest,
			Body:       testCase.responseBody,
		}

		httpClient := httpClientMock{Mock: &mock.Mock{}}
		httpClient.On("Do", expectedHTTPRequest).Return(&indexServiceResp, nil)

		indexService := NewBucketIndexRestService(&httpClient, "service://mock")

		bucketLocation := BucketLocation{Name: bucketName}
		metaData, err := indexService.Fetch(&bucketLocation)

		assert.Nil(t, metaData)
		assert.NotNil(t, err)
	}
}

func TestBucketMetaDataFetching(t *testing.T) {
	for _, testCase := range []struct {
		bucketName string
		visibility string
	}{
		{bucketName: "test", visibility: "internal"},
		{bucketName: "test", visibility: "internal"},
	} {

		expectedHTTPRequest, _ := http.NewRequest(
			http.MethodGet,
			fmt.Sprintf("service://mock/bucket/%s", testCase.bucketName),
			nil)

		metaDataJSON := fmt.Sprintf(`{"name": "%s", "visibility": "%s"}`, testCase.bucketName, testCase.visibility)

		indexServiceResp := http.Response{
			StatusCode: http.StatusOK,
			Request:    expectedHTTPRequest,
			Body:       ioutil.NopCloser(bytes.NewBuffer([]byte(metaDataJSON))),
		}

		httpClient := httpClientMock{Mock: &mock.Mock{}}
		httpClient.On("Do", expectedHTTPRequest).Return(&indexServiceResp, nil)

		indexService := NewBucketIndexRestService(&httpClient, "service://mock")

		bucketLocation := BucketLocation{Name: testCase.bucketName}
		expectedMetaData := BucketMetaData{Name: testCase.bucketName, IsInternal: testCase.visibility == internal}

		metaData, err := indexService.Fetch(&bucketLocation)

		assert.Equal(t, &expectedMetaData, metaData)
		assert.Nil(t, err)
	}
}

func (httpClient *httpClientMock) Do(request *http.Request) (*http.Response, error) {
	args := httpClient.Called(request)
	var response *http.Response
	if args.Get(0) != nil {
		response = args.Get(0).(*http.Response)
	}
	return response, args.Error(1)
}
