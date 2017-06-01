package regions

import (
	"testing"
	"github.com/allegro/akubra/sharding"
	"net/http"
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
	shardsMap := make(map[string]sharding.ShardsRingApi)
	regions := &Regions{
		multiCluters: shardsMap,
	}
	shardsRing := &sharding.ShardsRing{}
	regions.assignShardsRing("test1.qxlint", *shardsRing)
	request := &http.Request{Host:"test2.qxlint"}

	response, _ := regions.RoundTrip(request)

	assert.Equal(t, 404, response.StatusCode)
}

func TestShouldReturnResponseFromShardsRing(t *testing.T) {
	shardsMap := make(map[string]sharding.ShardsRingApi)
	regions := &Regions{
		multiCluters: shardsMap,
	}
	request := &http.Request{Host:"test1.qxlint"}
	expectedResponse := &http.Response{
		Status: "200 OK",
		StatusCode: 200,
		}
	shardsRingMock := &ShardsRingMock{}
	shardsRingMock.On("DoRequest", request).Return(expectedResponse)
	regions.assignShardsRing("test1.qxlint", shardsRingMock)

	response, _ := regions.RoundTrip(request)

	assert.Equal(t, 200, response.StatusCode)
	shardsRingMock.AssertCalled(t, "DoRequest", request)
}

func TestShouldReturnResponseFromShardsRingOnHostWithPort(t *testing.T) {
	shardsMap := make(map[string]sharding.ShardsRingApi)
	regions := &Regions{
		multiCluters: shardsMap,
	}
	request := &http.Request{Host:"test1.qxlint:1234"}
	expectedResponse := &http.Response{
		Status: "200 OK",
		StatusCode: 200,
	}
	shardsRingMock := &ShardsRingMock{}
	shardsRingMock.On("DoRequest", request).Return(expectedResponse)
	regions.assignShardsRing("test1.qxlint", shardsRingMock)

	response, _ := regions.RoundTrip(request)

	assert.Equal(t, 200, response.StatusCode)
}
