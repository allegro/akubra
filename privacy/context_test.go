package privacy

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/allegro/akubra/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type supplierMock struct {
	*mock.Mock
}

type roundTripperMock struct {
	*mock.Mock
}

func TestShouldSupplyRequestWithPrivacyContext(t *testing.T) {

	config := prepareConfig()
	supplier := NewBasicPrivacyContextSupplier(config)

	for _, isInternalNetworkRequest := range []bool{false, true} {
		req, err := http.NewRequest(http.MethodGet, "http://localhost:8080/bucket/object", nil)
		assert.Nil(t, err)

		if isInternalNetworkRequest {
			req.Header.Set(config.IsInternalNetworkHeaderName, config.IsInternalNetworkHeaderValue)
		}

		req, err = supplier.Supply(req)
		assert.Nil(t, err)

		privacyContext := req.Context().Value(RequestPrivacyContextKey).(*Context)
		if isInternalNetworkRequest {
			assert.True(t, privacyContext.isInternalNetwork)
		} else {
			assert.False(t, privacyContext.isInternalNetwork)
		}
	}
}

func TestShouldUseTheSupplierToSupplyTheRequestWithPrivacyConfig(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://localhost:8080/bucket/object", nil)
	assert.Nil(t, err)

	supplierMock := &supplierMock{Mock: &mock.Mock{}}
	supplierMock.On("Supply", req).Return(req, nil)

	expectedResp := &http.Response{StatusCode: http.StatusOK}
	rtMock := &roundTripperMock{Mock: &mock.Mock{}}
	rtMock.On("RoundTrip", req).Return(expectedResp, nil)

	supplierRT := NewPrivacyContextSupplierRoundTripper(rtMock, supplierMock)
	resp, err := supplierRT.RoundTrip(req)

	supplierMock.AssertCalled(t, "Supply", req)
	rtMock.AssertCalled(t, "RoundTrip", req)

	assert.Nil(t, err)
	assert.Equal(t, resp, expectedResp)
}

func TestShouldFailIfTheRequestCannoutBeSuppliedWithPrivacyContext(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://localhost:8080/bucket/object", nil)
	req = req.WithContext(context.WithValue(context.Background(), log.ContextreqIDKey, "123"))
	assert.Nil(t, err)

	supplierMock := &supplierMock{Mock: &mock.Mock{}}
	supplierMock.On("Supply", req).Return(nil, errors.New("fail"))

	rtMock := &roundTripperMock{Mock: &mock.Mock{}}

	supplierRT := NewPrivacyContextSupplierRoundTripper(rtMock, supplierMock)
	resp, err := supplierRT.RoundTrip(req)

	supplierMock.AssertCalled(t, "Supply", req)
	rtMock.AssertNotCalled(t, "RoundTrip", req)

	assert.Nil(t, resp)
	assert.Equal(t, err.Error(), "failed to supply request 123 with privacy context, reason: fail")
}

func (sm *supplierMock) Supply(req *http.Request) (*http.Request, error) {
	args := sm.Called(req)
	var r *http.Request
	if args.Get(0) != nil {
		r = args.Get(0).(*http.Request)
	}
	return r, args.Error(1)
}

func (rtm *roundTripperMock) RoundTrip(req *http.Request) (*http.Response, error) {
	args := rtm.Called(req)
	var resp *http.Response
	if args.Get(0) != nil {
		resp = args.Get(0).(*http.Response)
	}
	return resp, args.Error(1)
}

func prepareConfig() *Config {
	return &Config{
		IsInternalNetworkHeaderName:  "X-Internal",
		IsInternalNetworkHeaderValue: "1",
	}
}
