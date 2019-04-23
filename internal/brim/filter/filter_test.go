package filter

import (
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/AdRoll/goamz/aws"
	"github.com/AdRoll/goamz/s3"
	"github.com/allegro/akubra/internal/akubra/config"
	httpConfig "github.com/allegro/akubra/internal/akubra/httphandler/config"
	regionsConfig "github.com/allegro/akubra/internal/akubra/regions/config"
	"github.com/allegro/akubra/internal/akubra/sharding"
	storagesConfig "github.com/allegro/akubra/internal/akubra/storages/config"
	transportConfig "github.com/allegro/akubra/internal/akubra/transport/config"
	"github.com/allegro/akubra/internal/akubra/types"
	"github.com/allegro/akubra/internal/akubra/watchdog"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/allegro/akubra/internal/brim/auth"
	"github.com/allegro/akubra/internal/brim/internal/model"
	brimS3 "github.com/allegro/akubra/internal/brim/s3"
)

type versionFetcherMock struct {
	mock.Mock
}

type backendResolverMock struct {
	mock.Mock
}

func (resolver *backendResolverMock) ResolveClientForHost(hostURL, key, access string) (*s3.S3, error) {
	args := resolver.Called(hostURL, key, access)
	var client *s3.S3
	v := args.Get(0)
	if v != nil {
		client = v.(*s3.S3)
	}
	return client, args.Error(1)
}

func (resolver *backendResolverMock) ResolveClientForBackend(backendName, key, access string) (*s3.S3, error) {
	args := resolver.Called(backendName, key, access)
	var client *s3.S3
	v := args.Get(0)
	if v != nil {
		client = v.(*s3.S3)
	}
	return client, args.Error(1)
}

func (resolver *backendResolverMock) GetShardsRing(domain string) (sharding.ShardsRingAPI, error) {
	args := resolver.Called(domain)
	var ring sharding.ShardsRingAPI
	v := args.Get(0)
	if v != nil {
		ring = v.(sharding.ShardsRingAPI)
	}
	return ring, args.Error(1)
}

func (fetcherMock *versionFetcherMock) Fetch(auth *brimS3.MigrationAuth, bucketName string, key string) (*StorageState, error) {
	args := fetcherMock.Called(auth, bucketName, key)
	var state *StorageState
	v := args.Get(0)
	if v != nil {
		state = v.(*StorageState)
	}
	return state, args.Error(1)
}

func TestShouldFailedWhenRingCanNotBeResolved(t *testing.T) {
	resolver := &backendResolverMock{}
	versionFetcher := &versionFetcherMock{}

	filter := NewDefaultWALFilter(resolver, versionFetcher)

	entryWG := sync.WaitGroup{}
	entryWG.Add(1)

	walEntriesChannel := make(chan *model.WALEntry, 1)
	resolver.On("GetShardsRing", "test.qxlint").Return(nil, errors.New("No region with such domain exists"))

	var taskError error

	walEntriesChannel <- &model.WALEntry{Record: &watchdog.ConsistencyRecord{
		Domain:        "test.qxlint",
		ObjectID:      "some/key1",
		ObjectVersion: time.Now().UTC().Format(watchdog.VersionDateLayout)},
		RecordProcessedHook: func(_ *watchdog.ConsistencyRecord, err error) error {
			time.Sleep(2 * time.Second)
			defer entryWG.Done()
			taskError = err
			return nil
		}}

	filter.Filter(walEntriesChannel)
	entryWG.Wait()
	assert.Equal(t, taskError.Error(), "failed to resolve ring: No region with such domain exists")
}

func TestShouldGenerateANoopTaskWhenThereIsANewerVersionOfTheObjectAlreadyUploaded(t *testing.T) {
	akubraConfig := generateAkubraConfig(3)
	resolver := &backendResolverMock{}
	versionFetcher := &versionFetcherMock{}

	filter := NewDefaultWALFilter(resolver, versionFetcher)

	entryWG := sync.WaitGroup{}
	entryWG.Add(1)

	walEntriesChannel := make(chan *model.WALEntry, 1)
	shardsRing, _, _ := auth.Ring(akubraConfig, "test")

	resolver.
		On("GetShardsRing", "localhost").
		Return(shardsRing, nil)

	prepareMocksForStorages(resolver, akubraConfig.Storages, "123", "321", "some/key1")

	walEntry := &model.WALEntry{Record: &watchdog.ConsistencyRecord{
		Domain:        "localhost",
		ObjectID:      "some/key1",
		AccessKey:     "123",
		ObjectVersion: time.Now().UTC().Format(watchdog.VersionDateLayout)},
		RecordProcessedHook: func(_ *watchdog.ConsistencyRecord, err error) error { entryWG.Done(); assert.Nil(t, err); return nil }}

	walEntriesChannel <- walEntry
	prepareVersionMocks("some", "key1", "123", "321", versionFetcher, map[string]*StorageState{
		"http://localhost:1000": {storageEndpoint: "http://localhost:1000", version: time.Now().Add(-1 * time.Second).Format(watchdog.VersionDateLayout)},
		"http://localhost:2000": {storageEndpoint: "http://localhost:2000", version: time.Now().Add(-1 * time.Second).Format(watchdog.VersionDateLayout)},
		"http://localhost:3000": {storageEndpoint: "http://localhost:3000", version: time.Now().Format(watchdog.VersionDateLayout)},
	})

	tasksChannel := filter.Filter(walEntriesChannel)
	task := <-tasksChannel
	task.WALEntry.RecordProcessedHook(walEntry.Record, nil)

	entryWG.Wait()
	assert.Nil(t, task.SourceClient)
	assert.Empty(t, task.DestinationsClients)
}

func TestShouldGenerateMigrationsForStoragesWithoutObjectInProperVersion(t *testing.T) {
	akubraConfig := generateAkubraConfig(4)
	resolver := &backendResolverMock{}
	versionFetcher := &versionFetcherMock{}

	filter := NewDefaultWALFilter(resolver, versionFetcher)

	entryWG := sync.WaitGroup{}
	entryWG.Add(1)

	walEntriesChannel := make(chan *model.WALEntry, 1)
	shardsRing, _, _ := auth.Ring(akubraConfig, "test")

	resolver.
		On("GetShardsRing", "localhost").
		Return(shardsRing, nil)

	prepareMocksForStorages(resolver, akubraConfig.Storages, "123", "321", "some/key1")

	latestVersion := time.Now().Format(watchdog.VersionDateLayout)
	entry := &model.WALEntry{Record: &watchdog.ConsistencyRecord{
		Method:        watchdog.PUT,
		Domain:        "localhost",
		ObjectID:      "some/key1",
		AccessKey:     "123",
		ObjectVersion: latestVersion},
		RecordProcessedHook: func(_ *watchdog.ConsistencyRecord, err error) error { entryWG.Done(); assert.Nil(t, err); return nil }}

	walEntriesChannel <- entry
	prepareVersionMocks("some", "key1", "123", "321", versionFetcher, map[string]*StorageState{
		"http://localhost:1000": {storageEndpoint: "http://localhost:1000", version: time.Now().Add(-5 * time.Second).Format(watchdog.VersionDateLayout)},
		"http://localhost:2000": {storageEndpoint: "http://localhost:2000", objectNotFound: true},
		"http://localhost:3000": {storageEndpoint: "http://localhost:3000", version: ""},
		"http://localhost:4000": {storageEndpoint: "http://localhost:4000", version: latestVersion},
	})

	tasksChannel := filter.Filter(walEntriesChannel)
	task := <-tasksChannel
	task.WALEntry.RecordProcessedHook(entry.Record, nil)

	var dstEndpoints []string
	for _, cli := range task.DestinationsClients {
		dstEndpoints = append(dstEndpoints, cli.S3Endpoint)
	}

	entryWG.Wait()
	assert.Equal(t, task.SourceClient.S3Endpoint, "http://localhost:4000")
	assert.Len(t, task.DestinationsClients, 3)
	assert.Contains(t, dstEndpoints, "http://localhost:1000", "http://localhost:2000", "http://localhost:3000")
}

func TestShouldNotGenerateDeleteTasksIfTheObjectIsAlreadyAbsentOnAllStorages(t *testing.T) {
	akubraConfig := generateAkubraConfig(4)
	resolver := &backendResolverMock{}
	versionFetcher := &versionFetcherMock{}

	filter := NewDefaultWALFilter(resolver, versionFetcher)

	entryWG := sync.WaitGroup{}
	entryWG.Add(1)

	walEntriesChannel := make(chan *model.WALEntry, 1)
	shardsRing, _, _ := auth.Ring(akubraConfig, "test")

	resolver.
		On("GetShardsRing", "localhost").
		Return(shardsRing, nil)

	prepareMocksForStorages(resolver, akubraConfig.Storages, "123", "321", "some/key1")

	latestVersion := time.Now().Format(watchdog.VersionDateLayout)
	entry := &model.WALEntry{Record: &watchdog.ConsistencyRecord{
		Method:        watchdog.DELETE,
		Domain:        "localhost",
		ObjectID:      "some/key1",
		AccessKey:     "123",
		ObjectVersion: latestVersion},
		RecordProcessedHook: func(_ *watchdog.ConsistencyRecord, err error) error { entryWG.Done(); assert.Nil(t, err); return nil }}

	walEntriesChannel <- entry
	prepareVersionMocks("some", "key1", "123", "321", versionFetcher, map[string]*StorageState{
		"http://localhost:1000": {storageEndpoint: "http://localhost:1000", objectNotFound: true},
		"http://localhost:2000": {storageEndpoint: "http://localhost:2000", objectNotFound: true},
		"http://localhost:3000": {storageEndpoint: "http://localhost:3000", objectNotFound: true},
		"http://localhost:4000": {storageEndpoint: "http://localhost:4000", objectNotFound: true},
	})

	tasksChannel := filter.Filter(walEntriesChannel)
	task := <-tasksChannel
	task.WALEntry.RecordProcessedHook(entry.Record, nil)

	entryWG.Wait()
	assert.Nil(t, task.SourceClient)
	assert.Len(t, task.DestinationsClients, 0)
}

func TestShouldDeleteObjectsFromStoragesThatSillContainThem(t *testing.T) {
	akubraConfig := generateAkubraConfig(4)
	resolver := &backendResolverMock{}
	versionFetcher := &versionFetcherMock{}

	filter := NewDefaultWALFilter(resolver, versionFetcher)

	entryWG := sync.WaitGroup{}
	entryWG.Add(1)

	walEntriesChannel := make(chan *model.WALEntry, 1)
	shardsRing, _, _ := auth.Ring(akubraConfig, "test")

	resolver.
		On("GetShardsRing", "localhost").
		Return(shardsRing, nil)

	prepareMocksForStorages(resolver, akubraConfig.Storages, "123", "321", "some/key1")

	latestVersion := time.Now().Format(watchdog.VersionDateLayout)
	someOtherVersion := time.Now().Add(-5 * time.Minute).Format(watchdog.VersionDateLayout)

	entry := &model.WALEntry{Record: &watchdog.ConsistencyRecord{
		Method:        watchdog.DELETE,
		Domain:        "localhost",
		ObjectID:      "some/key1",
		AccessKey:     "123",
		ObjectVersion: latestVersion},
		RecordProcessedHook: func(_ *watchdog.ConsistencyRecord, err error) error { entryWG.Done(); assert.Nil(t, err); return nil }}

	walEntriesChannel <- entry
	prepareVersionMocks("some", "key1", "123", "321", versionFetcher, map[string]*StorageState{
		"http://localhost:1000": {storageEndpoint: "http://localhost:1000", version: ""},
		"http://localhost:2000": {storageEndpoint: "http://localhost:2000", version: latestVersion},
		"http://localhost:3000": {storageEndpoint: "http://localhost:3000", version: someOtherVersion},
		"http://localhost:4000": {storageEndpoint: "http://localhost:4000", objectNotFound: true},
	})

	tasksChannel := filter.Filter(walEntriesChannel)
	task := <-tasksChannel
	task.WALEntry.RecordProcessedHook(entry.Record, nil)

	var dstEndpoints []string
	for _, cli := range task.DestinationsClients {
		dstEndpoints = append(dstEndpoints, cli.S3Endpoint)
	}

	entryWG.Wait()
	assert.Nil(t, task.SourceClient)
	assert.Len(t, dstEndpoints, 3)
	assert.Contains(t, dstEndpoints, "http://localhost:1000", "http://localhost:2000", "http://localhost:3000")
}

func TestShoulKeepTheVersionFromStoragesIfTheVersionHeaderIsMissingOnAllStorages(t *testing.T) {
	akubraConfig := generateAkubraConfig(2)
	resolver := &backendResolverMock{}
	versionFetcher := &versionFetcherMock{}

	filter := NewDefaultWALFilter(resolver, versionFetcher)

	entryWG := sync.WaitGroup{}
	entryWG.Add(1)

	walEntriesChannel := make(chan *model.WALEntry, 1)
	shardsRing, _, _ := auth.Ring(akubraConfig, "test")

	resolver.
		On("GetShardsRing", "localhost").
		Return(shardsRing, nil)

	prepareMocksForStorages(resolver, akubraConfig.Storages, "123", "321", "some/key1")

	latestVersion := time.Now().Format(watchdog.VersionDateLayout)

	entry := &model.WALEntry{Record: &watchdog.ConsistencyRecord{
		Method:        watchdog.DELETE,
		Domain:        "localhost",
		ObjectID:      "some/key1",
		AccessKey:     "123",
		ObjectVersion: latestVersion},
		RecordProcessedHook: func(_ *watchdog.ConsistencyRecord, err error) error { entryWG.Done(); assert.Nil(t, err); return nil }}

	walEntriesChannel <- entry
	prepareVersionMocks("some", "key1", "123", "321", versionFetcher, map[string]*StorageState{
		"http://localhost:1000": {storageEndpoint: "http://localhost:1000", version: ""},
		"http://localhost:2000": {storageEndpoint: "http://localhost:2000", version: ""},
	})

	tasksChannel := filter.Filter(walEntriesChannel)
	task := <-tasksChannel
	task.WALEntry.RecordProcessedHook(entry.Record, nil)

	entryWG.Wait()
	assert.Nil(t, task.SourceClient)
	assert.Len(t, task.DestinationsClients, 0)
}

func prepareMocksForStorages(resolverMock *backendResolverMock, storagesMaps storagesConfig.StoragesMap, accessKey, secretKey, key string) {
	for storageName := range storagesMaps {
		s3Client := &s3.S3{Auth: aws.Auth{AccessKey: accessKey, SecretKey: secretKey}}
		resolverMock.
			On("ResolveClientForBackend", storageName, key, accessKey).
			Return(s3Client, nil)
	}
}

func prepareVersionMocks(bucket, key, access, secret string, fetcherMock *versionFetcherMock, states map[string]*StorageState) {
	for endpoint, state := range states {
		migrationAuth := &brimS3.MigrationAuth{Endpoint: endpoint, AccessKey: access, SecretKey: secret}
		fetcherMock.
			On("Fetch", migrationAuth, bucket, key).
			Return(state, nil)
	}
}

func generateAkubraConfig(numberOfStorages int) *config.Config {
	akubraConfig := &config.Config{}
	akubraConfig.Storages = storagesConfig.StoragesMap{}
	akubraConfig.Shards = storagesConfig.ShardsMap{}

	akubraConfig.ShardingPolicies = regionsConfig.ShardingPolicies{}
	akubraConfig.ShardingPolicies["test"] = regionsConfig.Policies{
		Domains: []string{"localhost"},
		Shards:  []regionsConfig.Policy{{Weight: 1, ShardName: "test"}},
	}

	akubraConfig.Service = httpConfig.Service{
		Client: httpConfig.Client{Transports: transportConfig.Transports{{}}}}

	var storages storagesConfig.Storages

	for i := 1; i <= numberOfStorages; i++ {
		endpoint, _ := url.Parse(fmt.Sprintf("http://localhost:%d", i*1000))

		akubraConfig.Storages[fmt.Sprintf("test-%d", i)] = storagesConfig.Storage{
			Maintenance: false,
			Backend:     types.YAMLUrl{URL: endpoint},
			Type:        "passthrough",
		}

		storages = append(storages, storagesConfig.StorageBreakerProperties{Name: fmt.Sprintf("test-%d", i)})
	}

	akubraConfig.Shards["test"] = storagesConfig.Shard{
		Storages: storages,
	}

	return akubraConfig
}
