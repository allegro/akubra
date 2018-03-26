package regions

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/allegro/akubra/sharding"
	"github.com/allegro/akubra/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type ShardsRingMock struct {
	mock.Mock
}

func (sro *ShardsRingMock) DoRequest(req *http.Request) (resp *http.Response, rerr error) {
	args := sro.Called(req)
	httpResponse := args.Get(0).(*http.Response)
	return httpResponse, nil
}

func TestCode404OnNotSupportedDomain(t *testing.T) {
	shardsMap := make(map[string]sharding.ShardsRingAPI)
	regions := &Regions{
		multiCluters: shardsMap,
	}
	shardsRing := &sharding.ShardsRing{}
	regions.assignShardsRing("test1.qxlint", *shardsRing)
	request := &http.Request{Host: "test2.qxlint"}

	response, _ := regions.RoundTrip(request)

	assert.Equal(t, 404, response.StatusCode)
}

func TestShouldReturnResponseFromShardsRingOnHostWithPort(t *testing.T) {
	shardsMap := make(map[string]sharding.ShardsRingAPI)
	regions := &Regions{
		multiCluters: shardsMap,
	}
	request := &http.Request{Host: "test1.qxlint:1234", Header: map[string][]string{}}
	expectedResponse := &http.Response{
		Status:     "200 OK",
		StatusCode: 200,
	}
	shardsRingMock := &ShardsRingMock{}
	shardsRingMock.On("DoRequest", request).Return(expectedResponse)
	regions.assignShardsRing("test1.qxlint", shardsRingMock)

	response, _ := regions.RoundTrip(request)

	assert.Equal(t, 200, response.StatusCode)
}

func TestShouldDetectADomainStyleRequestAndExtractBucketNameFromHost(t *testing.T) {
	regions := &Regions{
		multiCluters: make(map[string]sharding.ShardsRingAPI),
	}

	originalRequest := &http.Request{Host: "bucket.test.qxlint", Header: map[string][]string{}}
	interceptedRequest := &http.Request{Host: "bucket.test.qxlint", Header: map[string][]string{}}
	interceptedRequest.Header.Add(utils.InternalBucketHeader, "bucket")

	expectedResponse := &http.Response{
		Status:     "200 OK",
		StatusCode: 200,
	}
	shardsRingMock := &ShardsRingMock{}
	shardsRingMock.On("DoRequest", interceptedRequest).Return(expectedResponse)
	regions.assignShardsRing("test.qxlint", shardsRingMock)

	response, _ := regions.RoundTrip(originalRequest)

	assert.Equal(t, 200, response.StatusCode)
	assert.Equal(t, "", originalRequest.Header.Get(utils.InternalPathStyleFlag))
	assert.Equal(t,"bucket", originalRequest.Header.Get(utils.InternalBucketHeader))
}

func TestShouldDetectADomainStyleRequestAndExtractMultiLabelBucketNameFromHost(t *testing.T) {
	regions := &Regions{
		multiCluters: make(map[string]sharding.ShardsRingAPI),
	}

	originalRequest := &http.Request{Host: "sub.bucket.test.qxlint", Header: map[string][]string{}}
	interceptedRequest := &http.Request{Host: "sub.bucket.test.qxlint", Header: map[string][]string{}}
	assert.Equal(t, "", originalRequest.Header.Get(utils.InternalPathStyleFlag))
	interceptedRequest.Header.Add(utils.InternalBucketHeader, "sub.bucket")

	expectedResponse := &http.Response{
		Status:     "200 OK",
		StatusCode: 200,
	}
	shardsRingMock := &ShardsRingMock{}

	regions.assignShardsRing("test.qxlint", shardsRingMock)
	shardsRingMock.On("DoRequest", interceptedRequest).Return(expectedResponse)

	response, _ := regions.RoundTrip(originalRequest)

	assert.Equal(t, 200, response.StatusCode)
	assert.Equal(t, "", originalRequest.Header.Get(utils.InternalPathStyleFlag))
	assert.Equal(t,"sub.bucket", originalRequest.Header.Get(utils.InternalBucketHeader))
}

func TestShouldDetectADomainStyleRequestButFailOnMissingRegions(t *testing.T) {
	regions := &Regions{
		multiCluters: make(map[string]sharding.ShardsRingAPI),
	}

	requestWithNotSupportedDomain:= &http.Request{Host: "bucket.test.qxlint", Header: map[string][]string{}}
	expectedResponse := &http.Response{
		Status:     "404 Bad request",
		StatusCode: 404,
	}

	shardsRingMock := &ShardsRingMock{}
	shardsRingMock.On("DoRequest", requestWithNotSupportedDomain).Return(expectedResponse)
	regions.assignShardsRing("test2.qxlint", shardsRingMock)

	response, _ := regions.RoundTrip(requestWithNotSupportedDomain)

	assert.Equal(t, 404, response.StatusCode)
	assert.Equal(t, "", requestWithNotSupportedDomain.Header.Get(utils.InternalPathStyleFlag))
	assert.Empty(t, requestWithNotSupportedDomain.Header.Get(utils.InternalBucketHeader))
}

func TestShouldDetectAPathStyleRequestAndSetTheHeaderFlag(t *testing.T) {
	regions := &Regions{
		multiCluters: make(map[string]sharding.ShardsRingAPI),
	}

	requestURL, _ := url.Parse("http://test.qxlint:8080/bucket/object")
	requestWithNotSupportedDomain:= &http.Request{URL: requestURL, Host: "test.qxlint", Header: map[string][]string{}}
	expectedResponse := &http.Response{
		Status:     "OK",
		StatusCode: 200,
	}

	shardsRingMock := &ShardsRingMock{}
	shardsRingMock.On("DoRequest", requestWithNotSupportedDomain).Return(expectedResponse)
	regions.assignShardsRing("test.qxlint", shardsRingMock)

	response, _ := regions.RoundTrip(requestWithNotSupportedDomain)

	assert.Equal(t, 200, response.StatusCode)
	assert.Equal(t, "y", requestWithNotSupportedDomain.Header.Get(utils.InternalPathStyleFlag))
	assert.Empty(t, requestWithNotSupportedDomain.Header.Get(utils.InternalBucketHeader))
}