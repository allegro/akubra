package utils

import (
	"context"
	"net/http"
	"net/url"
	"testing"

	"github.com/allegro/akubra/log"
	"github.com/stretchr/testify/assert"
)

func TestShouldFailWhenRequestShouldBeRewrittenToPathStyleButBucketHeaderIsMissing(t *testing.T) {
	backendEndpoint, _ := url.Parse("http://localhost:8080")

	domainStyleReqURL, _ := url.Parse("http://my.bucket.test.qxlint/object")
	domainStyleRequest := &http.Request{URL: domainStyleReqURL, Header: map[string][]string{}}
	domainStyleRequest = domainStyleRequest.WithContext(context.WithValue(context.Background(), log.ContextreqIDKey, 123))

	err  := RewriteHostAndBucket(domainStyleRequest, backendEndpoint, true)

	assert.Equal(t, err.Error(), "missing bucket header, can't rewrite to path style, request id 123")
}

func TestShouldRewriteDomainStyleToPathStyleWhenBucketHeaderIsPresent(t *testing.T) {
	backendEndpoint, _ := url.Parse("http://localhost:8080")

	domainStyleReqURL, _ := url.Parse("http://my.bucket.test.qxlint/object")
	domainStyleRequest := &http.Request{URL: domainStyleReqURL, Header: map[string][]string{}}
	domainStyleRequest.Header.Add(InternalBucketHeader, "my.bucket")
	domainStyleRequest = domainStyleRequest.WithContext(context.WithValue(context.Background(), log.ContextreqIDKey, 123))

	err  := RewriteHostAndBucket(domainStyleRequest, backendEndpoint, true)

	assert.Nil(t, err)
	assert.Equal(t, domainStyleRequest.Host, "localhost:8080")
	assert.Equal(t, domainStyleRequest.URL.Path, "/my.bucket/object")
	assert.Equal(t, domainStyleRequest.URL.Scheme, "http")
}

func TestShouldNotRewriteDomainStyleToPathStyleWhenRequestIsAlreadyPathStyle(t *testing.T) {
	backendEndpoint, _ := url.Parse("https://localhost:8080")

	pathStyleReqURL, _ := url.Parse("http://test.qxlint/bucket/object")
	pathStyleRequest := &http.Request{URL: pathStyleReqURL, Header: map[string][]string{}}
	pathStyleRequest.Header.Add(InternalPathStyleFlag, "y")
	pathStyleRequest = pathStyleRequest.WithContext(context.WithValue(context.Background(), log.ContextreqIDKey, 123))

	err  := RewriteHostAndBucket(pathStyleRequest, backendEndpoint, true)

	assert.Nil(t, err)
	assert.Equal(t, pathStyleRequest.Host, "localhost:8080")
	assert.Equal(t, pathStyleRequest.URL.Path, "/bucket/object")
	assert.Equal(t, pathStyleRequest.URL.Scheme, "https")
}

func TestShouldNotRewriteToPathStyleWhenBackendDoesNotForceIt(t *testing.T) {
	backendEndpoint, _ := url.Parse("http://localhost:8080")

	pathStyleReqURL, _ := url.Parse("http://test.qxlint/bucket/object")
	pathStyleRequest := &http.Request{URL: pathStyleReqURL, Header: map[string][]string{}}
	pathStyleRequest = pathStyleRequest.WithContext(context.WithValue(context.Background(), log.ContextreqIDKey, 123))

	domainStyleReqURL, _ := url.Parse("http://bucket.test.qxlint/object")
	domainStyleRequest := &http.Request{URL: domainStyleReqURL, Header: map[string][]string{}}
	domainStyleRequest = domainStyleRequest.WithContext(context.WithValue(context.Background(), log.ContextreqIDKey, 124))
	domainStyleRequest.Header.Set(InternalBucketHeader, "bucket")

	pathStyleErr := RewriteHostAndBucket(pathStyleRequest, backendEndpoint, false)
	domainStyleErr  := RewriteHostAndBucket(domainStyleRequest, backendEndpoint, false)

	assert.Nil(t, pathStyleErr, domainStyleErr)
	assert.Equal(t, pathStyleRequest.Host, "localhost:8080")
	assert.Equal(t, pathStyleRequest.URL.Path, "/bucket/object")
	assert.Equal(t, pathStyleRequest.URL.Scheme, "http")

	assert.Equal(t, domainStyleRequest.Host, "bucket.localhost:8080")
	assert.Equal(t, domainStyleRequest.URL.Path, "/object")
	assert.Equal(t, domainStyleRequest.URL.Scheme, "http")
}