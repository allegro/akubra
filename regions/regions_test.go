package regions

import (
	"net/http"
	"testing"

	"github.com/allegro/akubra/sharding"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/allegro/akubra/storages"
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

func TestShouldReturnResponseFromShardsRing(t *testing.T) {
	shardsMap := make(map[string]sharding.ShardsRingAPI)
	regions := &Regions{
		multiCluters: shardsMap,
	}
	request := &http.Request{Host: "test1.qxlint", Header: map[string][]string{}}
	expectedResponse := &http.Response{
		Status:     "200 OK",
		StatusCode: 200,
	}
	shardsRingMock := &ShardsRingMock{}
	shardsRingMock.On("DoRequest", request).Return(expectedResponse)
	regions.assignShardsRing("test1.qxlint", shardsRingMock)
	regions.defaultRing = shardsRingMock
	response, _ := regions.RoundTrip(request)

	request2 := &http.Request{Host: ""}
	assert.Equal(t, 200, response.StatusCode)
	shardsRingMock.AssertCalled(t, "DoRequest", request)

	shardsRingMock.On("DoRequest", request2).Return(expectedResponse)

	response2, _ := regions.RoundTrip(request2)
	assert.Equal(t, 200, response2.StatusCode)
	shardsRingMock.AssertCalled(t, "DoRequest", request2)
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
	interceptedRequest.Header.Add(storages.InternalHostHeader, "test.qxlint")
	interceptedRequest.Header.Add(storages.InternalBucketHeader, "bucket")

	expectedResponse := &http.Response{
		Status:     "200 OK",
		StatusCode: 200,
	}
	shardsRingMock := &ShardsRingMock{}
	shardsRingMock.On("DoRequest", interceptedRequest).Return(expectedResponse)
	regions.assignShardsRing("test.qxlint", shardsRingMock)

	response, _ := regions.RoundTrip(originalRequest)

	assert.Equal(t, 200, response.StatusCode)
	assert.Equal(t, "test.qxlint", originalRequest.Header.Get(storages.InternalHostHeader))
	assert.Equal(t,"bucket", originalRequest.Header.Get(storages.InternalBucketHeader))
}

func TestShouldUseTheLongestMatchingHost(t *testing.T) {
	regions := &Regions{
		multiCluters: make(map[string]sharding.ShardsRingAPI),
	}

	originalRequest := &http.Request{Host: "bucket.test.qxlint", Header: map[string][]string{}}
	interceptedRequest := &http.Request{Host: "bucket.test.qxlint", Header: map[string][]string{}}
	interceptedRequest.Header.Add(storages.InternalHostHeader, "test.qxlint")
	interceptedRequest.Header.Add(storages.InternalBucketHeader, "bucket")

	expectedResponse := &http.Response{
		Status:     "200 OK",
		StatusCode: 200,
	}
	shardsRingMock := &ShardsRingMock{}
	shardsRingMock.On("DoRequest", interceptedRequest).Return(expectedResponse)
	regions.assignShardsRing("test.qxlint", shardsRingMock)

	response, _ := regions.RoundTrip(originalRequest)

	assert.Equal(t, 200, response.StatusCode)
	assert.Equal(t, "test.qxlint", originalRequest.Header.Get(storages.InternalHostHeader))
	assert.Equal(t,"bucket", originalRequest.Header.Get(storages.InternalBucketHeader))
}

func TestShouldDetectADomainStyleRequestAndExtractMultiLabelBucketNameFromHost(t *testing.T) {
	regions := &Regions{
		multiCluters: make(map[string]sharding.ShardsRingAPI),
	}

	originalRequest := &http.Request{Host: "sub.bucket.subtest.test.qxlint", Header: map[string][]string{}}
	interceptedRequest := &http.Request{Host: "sub.bucket.subtest.test.qxlint", Header: map[string][]string{}}
	interceptedRequest.Header.Add(storages.InternalHostHeader, "subtest.test.qxlint")
	interceptedRequest.Header.Add(storages.InternalBucketHeader, "sub.bucket")

	expectedResponse := &http.Response{
		Status:     "200 OK",
		StatusCode: 200,
	}
	shardsRingMock := &ShardsRingMock{}
	shardsRingMock1 := &ShardsRingMock{}

	regions.assignShardsRing("test.qxlint", shardsRingMock)
	regions.assignShardsRing("subtest.test.qxlint", shardsRingMock1)
	shardsRingMock1.On("DoRequest", interceptedRequest).Return(expectedResponse)

	response, _ := regions.RoundTrip(originalRequest)

	assert.Equal(t, 200, response.StatusCode)
	assert.Equal(t, "subtest.test.qxlint", originalRequest.Header.Get(storages.InternalHostHeader))
	assert.Equal(t,"sub.bucket", originalRequest.Header.Get(storages.InternalBucketHeader))
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
	assert.Empty(t, requestWithNotSupportedDomain.Header.Get(storages.InternalHostHeader))
	assert.Empty(t, requestWithNotSupportedDomain.Header.Get(storages.InternalBucketHeader))
}