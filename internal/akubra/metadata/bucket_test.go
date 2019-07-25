package metadata

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/allegro/bigcache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type FetcherMock struct {
	*mock.Mock
}

type FakeHasher struct {
	hashes map[string]uint64
}

func TestFetchingAndDirectCachingBucketMetaData(t *testing.T) {
	bucketName := "bucket"
	bucketNameHash := uint64(123)
	bucketLocation := BucketLocation{Name: bucketName}
	expectedMetaData := BucketMetaData{
		Name:       bucketName,
		Pattern:    "",
		IsInternal: true}

	fetcherMock := FetcherMock{Mock: &mock.Mock{}}
	fetcherMock.On("Fetch", &bucketLocation).Return(&expectedMetaData, nil)

	mockedHashes := map[string]uint64{
		bucketName: bucketNameHash,
		evictKey:   9999}
	cacheConfig := prepareCacheConfig(0*time.Second, &FakeHasher{hashes: mockedHashes})

	metaDataCache, err := NewBucketMetaDataCache(cacheConfig, &fetcherMock)
	assert.Nil(t, err)

	for i := 0; i < 1000; i++ {
		metaData, err := metaDataCache.Fetch(&bucketLocation)
		assert.Nil(t, err)
		assert.Equal(t, expectedMetaData, *metaData)
	}

	fetcherMock.AssertNumberOfCalls(t, "Fetch", 1)
}

func TestFetchingAndPatternCachingBucketMetaData(t *testing.T) {

	bucketName := "bucket1"
	patternMatched := `bucket\d`
	patternHash := uint64(123)
	bucketLocation := BucketLocation{Name: bucketName}
	expectedMetaData := BucketMetaData{
		Name:       bucketName,
		Pattern:    patternMatched,
		IsInternal: true}

	fetcherMock := FetcherMock{Mock: &mock.Mock{}}
	fetcherMock.On("Fetch", &bucketLocation).Return(&expectedMetaData, nil)

	mockedHashes := map[string]uint64{
		patternMatched: patternHash,
		bucketName:     0,
		evictKey:       9999}
	cacheConfig := prepareCacheConfig(0*time.Second, &FakeHasher{hashes: mockedHashes})

	metaDataCache, err := NewBucketMetaDataCache(cacheConfig, &fetcherMock)
	assert.Nil(t, err)

	for i := 0; i < 1000; i++ {
		metaData, err := metaDataCache.Fetch(&bucketLocation)
		assert.Nil(t, err)
		assert.Equal(t, expectedMetaData, *metaData)
	}

	fetcherMock.AssertNumberOfCalls(t, "Fetch", 1)
}
func TestFetcherFailure(t *testing.T) {

	bucketName := "bucket1"
	bucketNameHash := uint64(1234)
	bucketLocation := BucketLocation{Name: bucketName}

	expectedError := errors.New("fetch failed")
	fetcherMock := FetcherMock{Mock: &mock.Mock{}}
	fetcherMock.On("Fetch", &bucketLocation).Return(nil, expectedError)

	mockedHashes := map[string]uint64{
		bucketName: bucketNameHash,
		evictKey:   9999}
	cacheConfig := prepareCacheConfig(0*time.Second, &FakeHasher{hashes: mockedHashes})

	metaDataCache, err := NewBucketMetaDataCache(cacheConfig, &fetcherMock)
	assert.Nil(t, err)

	metaData, err := metaDataCache.Fetch(&bucketLocation)
	assert.Nil(t, metaData)
	assert.Equal(t, expectedError, err)

	fetcherMock.AssertCalled(t, "Fetch", &bucketLocation)
}

func TestCacheInvalidation(t *testing.T) {

	bucketName := "bucket1"
	bucketNameHash := uint64(1234)

	bucketName1 := "bucketx"
	bucketNameHash1 := uint64(1235)

	matchedPattern := `bucket\d`
	patternHash := uint64(4567)

	bucketLocation := BucketLocation{Name: bucketName}
	bucketLocation1 := BucketLocation{Name: bucketName1}

	expectedMetaData := BucketMetaData{
		Name:       bucketName,
		Pattern:    matchedPattern,
		IsInternal: true}

	expectedMetaData1 := BucketMetaData{
		Name:       bucketName1,
		Pattern:    "",
		IsInternal: true}

	fetcherMock := FetcherMock{Mock: &mock.Mock{}}
	fetcherMock.On("Fetch", &bucketLocation).Return(&expectedMetaData, nil)
	fetcherMock.On("Fetch", &bucketLocation1).Return(&expectedMetaData1, nil)

	mockedHashes := map[string]uint64{
		bucketName:     bucketNameHash,
		bucketName1:    bucketNameHash1,
		matchedPattern: patternHash,
		evictKey:       9999}

	cacheConfig := prepareCacheConfig(1*time.Nanosecond, &FakeHasher{hashes: mockedHashes})

	metaDataCache, err := NewBucketMetaDataCache(cacheConfig, &fetcherMock)
	assert.Nil(t, err)

	metaData, err := metaDataCache.Fetch(&bucketLocation)
	assert.Nil(t, err)
	assert.Equal(t, expectedMetaData, *metaData)

	metaData1, err := metaDataCache.Fetch(&bucketLocation1)
	assert.Nil(t, err)
	assert.Equal(t, expectedMetaData1, *metaData1)

	for len(fetcherMock.Calls) < 3 {
		metaData, err := metaDataCache.Fetch(&bucketLocation)
		assert.Nil(t, err)
		assert.Equal(t, expectedMetaData, *metaData)
	}

	fetcherMock.AssertNumberOfCalls(t, "Fetch", 3)
}

func prepareCacheConfig(entryLifeWindow time.Duration, hasher bigcache.Hasher) *BucketMetaDataCacheConfig {
	return &BucketMetaDataCacheConfig{
		LifeWindow:       entryLifeWindow,
		MaxCacheSizeInMB: 1,
		ShardsCount:      1,
		Hasher:           hasher,
	}
}

func (fetcherMock *FetcherMock) Fetch(bucketLocation *BucketLocation) (*BucketMetaData, error) {
	args := fetcherMock.Called(bucketLocation)
	var metaData *BucketMetaData
	if args.Get(0) != nil {
		metaData = args.Get(0).(*BucketMetaData)
	}
	return metaData, args.Error(1)
}

func (hasher *FakeHasher) Sum64(key string) uint64 {
	hash, found := hasher.hashes[key]
	if !found {
		panic(fmt.Sprintf("no mock hash for key %s", key))
	}
	return hash
}
