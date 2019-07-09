package regions

import (
	"context"
	"github.com/allegro/akubra/internal/akubra/storages"
	"net/http"
	"testing"

	"github.com/allegro/akubra/internal/akubra/regions/config"
	"github.com/allegro/akubra/internal/akubra/sharding"
	"github.com/allegro/akubra/internal/akubra/watchdog"
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

func (sro *ShardsRingMock) GetRingProps() *sharding.RingProps {
	args := sro.Called()
	v := args.Get(0)
	if v != nil {
		return v.(*sharding.RingProps)
	}
	return nil
}

func (sro *ShardsRingMock) Pick(key string) (storages.NamedShardClient, error) {
	args := sro.Called()
	v := args.Get(0)
	if v != nil {
		return v.(storages.NamedShardClient), args.Error(1)
	}
	return nil, args.Error(1)
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
	requestWithHostSpecified := &http.Request{Host: "test1.qxlint"}
	expectedResponse := &http.Response{
		Status:     "200 OK",
		StatusCode: 200,
	}

	shardProps := &sharding.RingProps{
		ReadRepair:       false,
		ConsistencyLevel: config.None,
	}

	shardsRingMock := &ShardsRingMock{}
	readRepairVersion := ""
	multipart := false
	noErrors := true

	requestWithHostAndContext := requestWithHostSpecified.WithContext(context.WithValue(requestWithHostSpecified.Context(), watchdog.Domain, requestWithHostSpecified.Host))
	requestWithHostAndContext = requestWithHostAndContext.WithContext(context.WithValue(requestWithHostAndContext.Context(), watchdog.ConsistencyLevel, shardProps.ConsistencyLevel))
	requestWithHostAndContext = requestWithHostAndContext.WithContext(context.WithValue(requestWithHostAndContext.Context(), watchdog.NoErrorsDuringRequest, &noErrors))
	requestWithHostAndContext = requestWithHostAndContext.WithContext(context.WithValue(requestWithHostAndContext.Context(), watchdog.ReadRepairObjectVersion, &readRepairVersion))
	requestWithHostAndContext = requestWithHostAndContext.WithContext(context.WithValue(requestWithHostAndContext.Context(), watchdog.MultiPartUpload, &multipart))
	requestWithHostAndContext = requestWithHostAndContext.WithContext(context.WithValue(requestWithHostAndContext.Context(), watchdog.ReadRepair, shardProps.ReadRepair))

	shardsRingMock.On("DoRequest", requestWithHostAndContext).Return(expectedResponse)
	shardsRingMock.On("GetRingProps").Return(shardProps)

	regions.assignShardsRing("test1.qxlint", shardsRingMock)
	regions.defaultRing = shardsRingMock

	response, _ := regions.RoundTrip(requestWithHostSpecified)
	assert.Equal(t, 200, response.StatusCode)
	shardsRingMock.AssertCalled(t, "DoRequest", requestWithHostAndContext)

	defaultRegionRequest := &http.Request{Host: ""}

	defaultRequestWithContext := defaultRegionRequest.WithContext(context.WithValue(defaultRegionRequest.Context(), watchdog.Domain, defaultRegionRequest.Host))
	defaultRequestWithContext = defaultRequestWithContext.WithContext(context.WithValue(defaultRequestWithContext.Context(), watchdog.ConsistencyLevel, shardProps.ConsistencyLevel))
	defaultRequestWithContext = defaultRequestWithContext.WithContext(context.WithValue(defaultRequestWithContext.Context(), watchdog.NoErrorsDuringRequest, &noErrors))
	defaultRequestWithContext = defaultRequestWithContext.WithContext(context.WithValue(defaultRequestWithContext.Context(), watchdog.ReadRepairObjectVersion, &readRepairVersion))
	defaultRequestWithContext = defaultRequestWithContext.WithContext(context.WithValue(defaultRequestWithContext.Context(), watchdog.MultiPartUpload, &multipart))
	defaultRequestWithContext = defaultRequestWithContext.WithContext(context.WithValue(defaultRequestWithContext.Context(), watchdog.ReadRepair, shardProps.ReadRepair))

	shardsRingMock.On("DoRequest", defaultRequestWithContext).Return(expectedResponse)

	defaultRegionResponse, _ := regions.RoundTrip(defaultRegionRequest)

	assert.Equal(t, 200, defaultRegionResponse.StatusCode)
	shardsRingMock.AssertCalled(t, "DoRequest", defaultRequestWithContext)
}

func TestShouldReturnResponseFromShardsRingOnHostWithPort(t *testing.T) {
	shardsMap := make(map[string]sharding.ShardsRingAPI)
	regions := &Regions{
		multiCluters: shardsMap,
	}
	request := &http.Request{Host: "test1.qxlint:1234"}
	expectedResponse := &http.Response{
		Status:     "200 OK",
		StatusCode: 200,
	}
	shardsRingMock := &ShardsRingMock{}
	shardProps := &sharding.RingProps{
		ReadRepair:       false,
		ConsistencyLevel: config.None,
	}
	readRepairVersion := ""
	multipart := false
	noErrors := true

	requestWithContext := request.WithContext(context.WithValue(request.Context(), watchdog.Domain, "test1.qxlint"))
	requestWithContext = requestWithContext.WithContext(context.WithValue(requestWithContext.Context(), watchdog.ConsistencyLevel, shardProps.ConsistencyLevel))
	requestWithContext = requestWithContext.WithContext(context.WithValue(requestWithContext.Context(), watchdog.NoErrorsDuringRequest, &noErrors))
	requestWithContext = requestWithContext.WithContext(context.WithValue(requestWithContext.Context(), watchdog.ReadRepairObjectVersion, &readRepairVersion))
	requestWithContext = requestWithContext.WithContext(context.WithValue(requestWithContext.Context(), watchdog.MultiPartUpload, &multipart))
	requestWithContext = requestWithContext.WithContext(context.WithValue(requestWithContext.Context(), watchdog.ReadRepair, shardProps.ReadRepair))

	shardsRingMock.On("DoRequest", requestWithContext).Return(expectedResponse)
	shardsRingMock.On("GetRingProps").Return(shardProps)

	regions.assignShardsRing("test1.qxlint", shardsRingMock)

	response, _ := regions.RoundTrip(request)
	assert.Equal(t, 200, response.StatusCode)
}
