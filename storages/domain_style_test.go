package storages

import (
	"context"
	"net/http"
	"net/url"
	"testing"

	"github.com/allegro/akubra/log"
	"github.com/stretchr/testify/assert"
)

func TestShouldFailWhenTheCustomHostHeaderIsMissing(t *testing.T) {
	request := &http.Request{Header: map[string][]string{}}
	request = request.WithContext(context.WithValue(context.Background(), log.ContextreqIDKey, 123))

	domainStyleDecorator := domainStyleDecorator(&MockedRoundTripper{})

	response, err := domainStyleDecorator.RoundTrip(request)

	assert.Nil(t, response)
	assert.Equal(t, err.Error(), "Missing host header, request id 123")
}

func TestShouldNotAlterThePathWhenBucketNameIsEmpty(t *testing.T) {
	reqURL, _ := url.Parse("http://test.qxlint/bucket/object")
	originalRequest := &http.Request{URL: reqURL, Header: map[string][]string{}}
	originalRequest.Header.Add(InternalHostHeader, "test.qxlint")
	originalRequest = originalRequest.WithContext(context.WithValue(context.Background(), log.ContextreqIDKey, 123))

	interceptedRequest := &http.Request{}
	*interceptedRequest = *originalRequest
	interceptedRequest.Host = "test.qxlint"
	interceptedRequest.Header = map[string][]string{}

	expectedResponse := &http.Response{
		StatusCode: 200,
		Status:     "200 OK",
	}

	backendRoundTripper := &MockedRoundTripper{}
	backendRoundTripper.On("RoundTrip", interceptedRequest).Return(expectedResponse, nil)

	domainStyleDecorator := domainStyleDecorator(backendRoundTripper)
	response, err := domainStyleDecorator.RoundTrip(originalRequest)

	assert.Nil(t, err)
	assert.Equal(t, response.StatusCode, 200)
}

func TestShouldPrependBucketNameToPathWhenRequestIsDomainStyle(t *testing.T) {
	domainStyleURL, _ := url.Parse("http://bucket.test.qxlint/object")
	originalRequest := &http.Request{URL: domainStyleURL, Header: map[string][]string{}}
	originalRequest.Header.Add(InternalHostHeader, "test.qxlint")
	originalRequest.Header.Add(InternalBucketHeader, "bucket")

	originalRequest = originalRequest.WithContext(context.WithValue(context.Background(), log.ContextreqIDKey, 123))

	pathStyleURL, _ := url.Parse("http://test.qxlint/bucket/object")
	interceptedRequest := &http.Request{URL: pathStyleURL, Header: map[string][]string{}, Host: "test.qxlint"}
	interceptedRequest = interceptedRequest.WithContext(originalRequest.Context())

	expectedResponse := &http.Response{
		StatusCode: 200,
		Status:     "200 OK",
	}

	backendRoundTripper := &MockedRoundTripper{}
	backendRoundTripper.On("RoundTrip", interceptedRequest).Return(expectedResponse, nil)

	domainStyleDecorator := domainStyleDecorator(backendRoundTripper)
	response, err := domainStyleDecorator.RoundTrip(originalRequest)

	assert.Nil(t, err)
	assert.Equal(t, response.StatusCode, 200)
}

func TestShouldPrependBucketNameToPathWhenRequestIsDomainStyleAndOnlyBucketNameIsProvider(t *testing.T) {
	domainStyleURL, _ := url.Parse("http://bucket.test.qxlint")
	originalRequest := &http.Request{URL: domainStyleURL, Header: map[string][]string{}}
	originalRequest.Header.Add(InternalHostHeader, "test.qxlint")
	originalRequest.Header.Add(InternalBucketHeader, "bucket")

	originalRequest = originalRequest.WithContext(context.WithValue(context.Background(), log.ContextreqIDKey, 123))

	pathStyleURL, _ := url.Parse("http://test.qxlint/bucket")
	interceptedRequest := &http.Request{URL: pathStyleURL, Header: map[string][]string{}, Host: "test.qxlint"}
	interceptedRequest = interceptedRequest.WithContext(originalRequest.Context())

	expectedResponse := &http.Response{
		StatusCode: 200,
		Status:     "200 OK",
	}

	backendRoundTripper := &MockedRoundTripper{}
	backendRoundTripper.On("RoundTrip", interceptedRequest).Return(expectedResponse, nil)

	domainStyleDecorator := domainStyleDecorator(backendRoundTripper)
	response, err := domainStyleDecorator.RoundTrip(originalRequest)

	assert.Nil(t, err)
	assert.Equal(t, response.StatusCode, 200)
}


func TestShouldPrependBucketNameToPathWhenRequestIsDomainStyleAndQueryParamsAreProvider(t *testing.T) {
	domainStyleURL, _ := url.Parse("http://bucket.test.qxlint?param=321")
	originalRequest := &http.Request{URL: domainStyleURL, Header: map[string][]string{}}
	originalRequest.Header.Add(InternalHostHeader, "test.qxlint")
	originalRequest.Header.Add(InternalBucketHeader, "bucket")

	originalRequest = originalRequest.WithContext(context.WithValue(context.Background(), log.ContextreqIDKey, 123))

	pathStyleURL, _ := url.Parse("http://test.qxlint/bucket?param=321")
	interceptedRequest := &http.Request{URL: pathStyleURL, Header: map[string][]string{}, Host: "test.qxlint"}
	interceptedRequest = interceptedRequest.WithContext(originalRequest.Context())

	expectedResponse := &http.Response{
		StatusCode: 200,
		Status:     "200 OK",
	}

	backendRoundTripper := &MockedRoundTripper{}
	backendRoundTripper.On("RoundTrip", interceptedRequest).Return(expectedResponse, nil)

	domainStyleDecorator := domainStyleDecorator(backendRoundTripper)
	response, err := domainStyleDecorator.RoundTrip(originalRequest)

	assert.Nil(t, err)
	assert.Equal(t, response.StatusCode, 200)
}