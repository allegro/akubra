package metadata

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"hash/fnv"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/allegro/akubra/internal/akubra/log"
	"github.com/allegro/bigcache"
)

var evictKey = fmt.Sprintf("_%s_", strings.Repeat("x", 64))

//BucketMetaData is akubra-specific metadata about the bucket
type BucketMetaData struct {
	//Name is the name of the bucket
	Name string
	//IsInternal tells if bucket should be accessed from internal network only
	IsInternal bool
	//Pattern is the pattern that the name of the bucket was matched to
	Pattern string
}

//BucketLocation describes where to find the bucket
type BucketLocation struct {
	//Name is the name of the bucket
	Name string
}

//BucketMetaDataFetcher fetches metadata by BucketLocation
type BucketMetaDataFetcher interface {
	Fetch(bucketLocation *BucketLocation) (*BucketMetaData, error)
}

//BucketMetaDataCacheConfig holds the configuration for bucket's cache
type BucketMetaDataCacheConfig struct {
	//LifeWindow is time after which entry will be invalidated
	LifeWindow time.Duration `yaml:"LifeWindow"`
	//MaxCacheSizeInMB is the hard max that the cache will not exceed
	MaxCacheSizeInMB int `yaml:"MaxCacheSizeInMB"`
	//ShardsCount is the number of shards
	ShardsCount int `yaml:"ShardsCount"`
	//Hasher is the hash function that will be used to hash the keys
	Hasher bigcache.Hasher
}

//NewBucketMetaDataCache wraps the supplies fetcher with a cache layer
func NewBucketMetaDataCache(conf *BucketMetaDataCacheConfig, fetcher BucketMetaDataFetcher) (BucketMetaDataFetcher, error) {
	bigcacheConf := bigcache.Config{
		Shards:           conf.ShardsCount,
		LifeWindow:       conf.LifeWindow,
		Hasher:           conf.Hasher,
		HardMaxCacheSize: conf.MaxCacheSizeInMB,
	}

	bigCache, err := bigcache.NewBigCache(bigcacheConf)
	if err != nil {
		return nil, err
	}

	metaDataCache := &BucketMetaDataCache{
		bucketMetaDataFetcher:    fetcher,
		cache:                    bigCache,
		hasher:                   conf.Hasher,
		bucketNamePatternMapping: make(map[uint64]*regexp.Regexp),
		patterns:                 make([]*regexp.Regexp, 0),
		patternsLock:             sync.Mutex{},
		lifeWindow:               conf.LifeWindow,
	}

	go metaDataCache.evictExpired()
	return metaDataCache, nil
}

//BucketMetaDataCache is a wrapper that caches the answers given by the wrapped BucketMetaDataFetcher
type BucketMetaDataCache struct {
	bucketMetaDataFetcher    BucketMetaDataFetcher
	cache                    *bigcache.BigCache
	hasher                   bigcache.Hasher
	patterns                 []*regexp.Regexp
	bucketNamePatternMapping map[uint64]*regexp.Regexp
	patternsLock             sync.Mutex
	lifeWindow               time.Duration
}

//Fetch first consults the cache for BucketMetaData and only fetches it when it's not in the cache
func (bucketCache *BucketMetaDataCache) Fetch(bucketLocation *BucketLocation) (*BucketMetaData, error) {
	bucketMetaData := bucketCache.findByDirectMapping(bucketLocation.Name, false)
	if bucketMetaData != nil {
		return bucketMetaData, nil
	}
	bucketMetaData = bucketCache.findByPattern(bucketLocation.Name)
	if bucketMetaData != nil {
		return bucketMetaData, nil
	}
	return bucketCache.fetchAndCache(bucketLocation)
}

func (bucketCache *BucketMetaDataCache) findByDirectMapping(bucketName string, isPattern bool) *BucketMetaData {
	bucketMetaDataBytes, err := bucketCache.cache.Get(bucketName)
	if err != nil {
		return nil
	}
	buffer := bytes.NewBuffer(bucketMetaDataBytes)
	bucketMetaData, err := decodeBucketMetaData(buffer)
	if err != nil {
		log.Debugf("failed to decode metadata for bucket %s: %s", bucketName, err)
		_ = bucketCache.cache.Delete(bucketName)
		return nil
	}
	//hash collision handling
	if !isPattern && bucketMetaData.Name != bucketName {
		return nil
	}
	return bucketMetaData
}

func (bucketCache *BucketMetaDataCache) findByPattern(bucketName string) *BucketMetaData {
	bucketNameHash := bucketCache.hasher.Sum64(bucketName)
	metaData := bucketCache.findByBucketNameToPatternMapping(bucketNameHash)
	if metaData != nil {
		return metaData
	}
	return bucketCache.findPatternThatMatches(bucketName)
}

func (bucketCache *BucketMetaDataCache) findByBucketNameToPatternMapping(bucketNameHash uint64) *BucketMetaData {
	pattern, found := bucketCache.bucketNamePatternMapping[bucketNameHash]
	if found {
		metadata := bucketCache.findByDirectMapping(pattern.String(), true)
		if metadata != nil {
			return metadata
		}
		bucketCache.patternsLock.Lock()
		defer bucketCache.patternsLock.Unlock()
		delete(bucketCache.bucketNamePatternMapping, bucketNameHash)
	}
	return nil
}

func (bucketCache *BucketMetaDataCache) findPatternThatMatches(bucketName string) *BucketMetaData {
	var patterns []*regexp.Regexp
	patterns = bucketCache.patterns //to avoid strange behaviour during concurrent access
	for _, pattern := range patterns {
		matched := pattern.MatchString(bucketName)
		if matched {
			bucketCache.patternsLock.Lock()
			defer bucketCache.patternsLock.Unlock()
			bucketCache.bucketNamePatternMapping[bucketCache.hasher.Sum64(bucketName)] = pattern
			return bucketCache.findByDirectMapping(pattern.String(), true)
		}
	}
	return nil
}

func (bucketCache *BucketMetaDataCache) fetchAndCache(bucketLocation *BucketLocation) (*BucketMetaData, error) {
	metaData, err := bucketCache.bucketMetaDataFetcher.Fetch(bucketLocation)
	if err != nil {
		return nil, err
	}
	bucketCache.cacheResult(metaData)
	return metaData, nil
}

func (bucketCache *BucketMetaDataCache) cacheResult(metaData *BucketMetaData) {
	encodedMetaData, err := encodeBucketMetaData(metaData)
	if err != nil {
		log.Debugf("failed to cache result for bucket %s: %s", metaData.Name, err)
		return
	}
	if metaData.Pattern != "" {
		pattern, err := regexp.Compile(metaData.Pattern)
		if err == nil {
			bucketCache.patterns = append(bucketCache.patterns, pattern)
			_ = bucketCache.cache.Set(metaData.Pattern, encodedMetaData.Bytes())
		}
		return
	}
	_ = bucketCache.cache.Set(metaData.Name, encodedMetaData.Bytes())
}

func (bucketCache *BucketMetaDataCache) evictExpired() {
	if bucketCache.lifeWindow == 0 {
		return
	}
	for {
		_ = bucketCache.cache.Set(evictKey, []byte{})
		time.Sleep(bucketCache.lifeWindow)
	}
}

func decodeBucketMetaData(metaDataBytes *bytes.Buffer) (*BucketMetaData, error) {
	var bucketMetaData BucketMetaData
	decoder := gob.NewDecoder(metaDataBytes)
	if err := decoder.Decode(&bucketMetaData); err != nil {
		return nil, err
	}
	return &bucketMetaData, nil
}

func encodeBucketMetaData(metaData *BucketMetaData) (*bytes.Buffer, error) {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	if err := encoder.Encode(metaData); err != nil {
		return nil, err
	}
	return &buffer, nil
}

//Fnv64Hasher wraps the stdlib hasher to bigcache's Hasher type
type Fnv64Hasher struct{}

//Sum64 returns a hash for the key
func (h *Fnv64Hasher) Sum64(key string) uint64 {
	f := fnv.New64a()
	f.Sum([]byte(key))
	return f.Sum64()
}

//FakeBucketMetaDataFetcher always reports bucket as non-internal
type FakeBucketMetaDataFetcher struct{}

//Fetch just returns the BucketMetaData
func (fetcher *FakeBucketMetaDataFetcher) Fetch(BucketLocation *BucketLocation) (*BucketMetaData, error) {
	return &BucketMetaData{
		Pattern:    "",
		IsInternal: false,
		Name:       BucketLocation.Name,
	}, nil
}
