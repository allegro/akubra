package storages

import (
	"testing"
	"net/http"
	"github.com/stretchr/testify/assert"
	"context"
	"github.com/allegro/akubra/log"
	"net/url"
)

func TestShouldFailWhenTheCustomHostHeaderIsMissing(t *testing.T) {
	request := &http.Request{Header: map[string][]string{}}
	request = request.WithContext(context.WithValue(context.Background(), log.ContextreqIDKey, 123))

	domainStyleDecorator := domainStyleDecorator(&MockedRoundTripper{})

	response, err := domainStyleDecorator.RoundTrip(request)

	assert.Nil(t, response)
	assert.Equal(t, err.Error(), "Missing host header, request id 123!")
}

func TestShouldNotAlterThePathWhenBucketNameIsEmpty(t *testing.T) {
	reqUrl, _ := url.Parse("http://test.qxlint/bucket/object")
	originalRequest := &http.Request{URL: reqUrl, Header: map[string][]string{}}
	originalRequest.Header.Add(HOST, "test.qxlint")
	originalRequest = originalRequest.WithContext(context.WithValue(context.Background(), log.ContextreqIDKey, 123))

	interceptedRequest := &http.Request{}
	*interceptedRequest = *originalRequest
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
	domainStyleUrl, _ := url.Parse("http://bucket.test.qxlint/object")
	originalRequest := &http.Request{URL: domainStyleUrl, Header: map[string][]string{}}
	originalRequest.Header.Add(HOST, "test.qxlint")
	originalRequest.Header.Add(BUCKET, "bucket")

	originalRequest = originalRequest.WithContext(context.WithValue(context.Background(), log.ContextreqIDKey, 123))

	pathSyleUrl, _ := url.Parse("http://test.qxlint/bucket/object")
	interceptedRequest := &http.Request{URL: pathSyleUrl, Header: map[string][]string{}}
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