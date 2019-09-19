package filter

import (
	"fmt"
	"github.com/allegro/akubra/internal/akubra/types"
	"github.com/allegro/akubra/internal/brim/model"
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
	"github.com/allegro/akubra/internal/akubra/watchdog"
	"github.com/allegro/akubra/internal/brim/auth"
	brimS3 "github.com/allegro/akubra/internal/brim/s3"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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
		ObjectVersion: 1},
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
	akubraConfig := generateAkubraConfig(1, 3)
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
		ObjectVersion: 1},
		RecordProcessedHook: func(_ *watchdog.ConsistencyRecord, err error) error { entryWG.Done(); assert.Nil(t, err); return nil }}

	walEntriesChannel <- walEntry
	prepareVersionMocks("some", "key1", "123", "321", versionFetcher, map[string]*StorageState{
		"http://localhost:1000": {storageEndpoint: "http://localhost:1000", version: 1},
		"http://localhost:1100": {storageEndpoint: "http://localhost:1100", version: 1},
		"http://localhost:1200": {storageEndpoint: "http://localhost:1200", version: 2},
	})

	tasksChannel := filter.Filter(walEntriesChannel)
	task := <-tasksChannel
	_ = task.WALEntry.RecordProcessedHook(walEntry.Record, nil)

	entryWG.Wait()
	assert.Nil(t, task.SourceClient)
	assert.Empty(t, task.DestinationsClients)
}

func TestShouldGenerateMigrationsForStoragesWithoutObjectInProperVersion(t *testing.T) {
	akubraConfig := generateAkubraConfig(1, 4)
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

	latestVersion := 2
	entry := &model.WALEntry{Record: &watchdog.ConsistencyRecord{
		Method:        watchdog.PUT,
		Domain:        "localhost",
		ObjectID:      "some/key1",
		AccessKey:     "123",
		ObjectVersion: latestVersion},
		RecordProcessedHook: func(_ *watchdog.ConsistencyRecord, err error) error { entryWG.Done(); assert.Nil(t, err); return nil }}

	walEntriesChannel <- entry
	prepareVersionMocks("some", "key1", "123", "321", versionFetcher, map[string]*StorageState{
		"http://localhost:1000": {storageEndpoint: "http://localhost:1000", version: 1},
		"http://localhost:1100": {storageEndpoint: "http://localhost:1100", objectNotFound: true},
		"http://localhost:1200": {storageEndpoint: "http://localhost:1200", version: -1},
		"http://localhost:1300": {storageEndpoint: "http://localhost:1300", version: latestVersion},
	})

	tasksChannel := filter.Filter(walEntriesChannel)
	migrationTask := <-tasksChannel
	_ = migrationTask.WALEntry.RecordProcessedHook(entry.Record, nil)

	oldStoragesTask := <-tasksChannel
	_ = oldStoragesTask.WALEntry.RecordProcessedHook(entry.Record, nil)

	var dstEndpoints []string
	for _, cli := range migrationTask.DestinationsClients {
		dstEndpoints = append(dstEndpoints, cli.S3Endpoint)
	}

	entryWG.Wait()
	assert.Equal(t, migrationTask.SourceClient.S3Endpoint, "http://localhost:1300")
	assert.Len(t, migrationTask.DestinationsClients, 3)
	assert.Len(t, oldStoragesTask.DestinationsClients, 0)
	assert.Contains(t, dstEndpoints, "http://localhost:1000", "http://localhost:1100", "http://localhost:1200")
}

func TestShouldNotGenerateDeleteTasksIfTheObjectIsAlreadyAbsentOnAllStorages(t *testing.T) {
	akubraConfig := generateAkubraConfig(1, 4)
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

	latestVersion := 2
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
		"http://localhost:1100": {storageEndpoint: "http://localhost:1100", objectNotFound: true},
		"http://localhost:1200": {storageEndpoint: "http://localhost:1200", objectNotFound: true},
		"http://localhost:1300": {storageEndpoint: "http://localhost:1300", objectNotFound: true},
	})

	tasksChannel := filter.Filter(walEntriesChannel)
	task := <-tasksChannel
	_ = task.WALEntry.RecordProcessedHook(entry.Record, nil)

	entryWG.Wait()
	assert.Nil(t, task.SourceClient)
	assert.Len(t, task.DestinationsClients, 0)
}

func TestShouldDeleteObjectsFromStoragesThatSillContainThem(t *testing.T) {
	akubraConfig := generateAkubraConfig(1, 4)
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

	latestVersion := 2
	someOtherVersion := 1

	entry := &model.WALEntry{Record: &watchdog.ConsistencyRecord{
		Method:        watchdog.DELETE,
		Domain:        "localhost",
		ObjectID:      "some/key1",
		AccessKey:     "123",
		ObjectVersion: latestVersion},
		RecordProcessedHook: func(_ *watchdog.ConsistencyRecord, err error) error { entryWG.Done(); assert.Nil(t, err); return nil }}

	walEntriesChannel <- entry
	prepareVersionMocks("some", "key1", "123", "321", versionFetcher, map[string]*StorageState{
		"http://localhost:1000": {storageEndpoint: "http://localhost:1000", version: -1},
		"http://localhost:1100": {storageEndpoint: "http://localhost:1100", version: latestVersion},
		"http://localhost:1200": {storageEndpoint: "http://localhost:1200", version: someOtherVersion},
		"http://localhost:1300": {storageEndpoint: "http://localhost:1300", objectNotFound: true},
	})

	tasksChannel := filter.Filter(walEntriesChannel)
	task := <-tasksChannel
	_ = task.WALEntry.RecordProcessedHook(entry.Record, nil)

	var dstEndpoints []string
	for _, cli := range task.DestinationsClients {
		dstEndpoints = append(dstEndpoints, cli.S3Endpoint)
	}

	entryWG.Wait()
	assert.Nil(t, task.SourceClient)
	assert.Len(t, dstEndpoints, 3)
	assert.Contains(t, dstEndpoints, "http://localhost:1000", "http://localhost:1100", "http://localhost:1200")
}

func TestShoulKeepTheVersionFromStoragesIfTheVersionHeaderIsMissingOnAllStorages(t *testing.T) {
	akubraConfig := generateAkubraConfig(1, 2)
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

	latestVersion := 2

	entry := &model.WALEntry{Record: &watchdog.ConsistencyRecord{
		Method:        watchdog.DELETE,
		Domain:        "localhost",
		ObjectID:      "some/key1",
		AccessKey:     "123",
		ObjectVersion: latestVersion},
		RecordProcessedHook: func(_ *watchdog.ConsistencyRecord, err error) error { entryWG.Done(); assert.Nil(t, err); return nil }}

	walEntriesChannel <- entry
	prepareVersionMocks("some", "key1", "123", "321", versionFetcher, map[string]*StorageState{
		"http://localhost:1000": {storageEndpoint: "http://localhost:1000", version: -1},
		"http://localhost:1100": {storageEndpoint: "http://localhost:1100", version: -1},
	})

	tasksChannel := filter.Filter(walEntriesChannel)
	task := <-tasksChannel
	_ = task.WALEntry.RecordProcessedHook(entry.Record, nil)

	entryWG.Wait()
	assert.Nil(t, task.SourceClient)
	assert.Len(t, task.DestinationsClients, 0)
}


func TestShouldClearOldShardsFromObjectVersions(t *testing.T) {
	akubraConfig := generateAkubraConfig(2, 3)
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

	latestVersion := 4

	entry := &model.WALEntry{Record: &watchdog.ConsistencyRecord{
		Method:        watchdog.PUT,
		Domain:        "localhost",
		ObjectID:      "some/key1",
		AccessKey:     "123",
		ObjectVersion: latestVersion},
		RecordProcessedHook: func(_ *watchdog.ConsistencyRecord, err error) error { entryWG.Done(); assert.Nil(t, err); return nil }}

	walEntriesChannel <- entry
	prepareVersionMocks("some", "key1", "123", "321", versionFetcher, map[string]*StorageState{
		"http://localhost:1000": {storageEndpoint: "http://localhost:1000", version: 2},
		"http://localhost:1100": {storageEndpoint: "http://localhost:1100", version: 4},
		"http://localhost:1200": {storageEndpoint: "http://localhost:1200", objectNotFound: true},
		"http://localhost:2000": {storageEndpoint: "http://localhost:2000", version: 4},
		"http://localhost:2100": {storageEndpoint: "http://localhost:2100", version: 4},
		"http://localhost:2200": {storageEndpoint: "http://localhost:2200", version: 3},
	})

	tasksChannel := filter.Filter(walEntriesChannel)

	migrationTask := <-tasksChannel
	clearOldStoragesTasks := <-tasksChannel


	_ = migrationTask.WALEntry.RecordProcessedHook(entry.Record, nil)
	_ = clearOldStoragesTasks.WALEntry.RecordProcessedHook(entry.Record, nil)

	var migrationDstEndpoints []string
	for _, cli := range migrationTask.DestinationsClients {
		migrationDstEndpoints = append(migrationDstEndpoints, cli.S3Endpoint)
	}

	var endpointsToClear []string
	for _, cli := range clearOldStoragesTasks.DestinationsClients {
		endpointsToClear = append(endpointsToClear, cli.S3Endpoint)
	}

	entryWG.Wait()
	assert.True(t, migrationTask.SourceClient.S3Endpoint == "http://localhost:2100" || migrationTask.SourceClient.S3Endpoint == "http://localhost:2000")
	assert.Equal(t, clearOldStoragesTasks.WALEntry.Record.Method, watchdog.DELETE)
	assert.Equal(t, migrationDstEndpoints, []string{"http://localhost:2200"})
	assert.Equal(t, endpointsToClear, []string{"http://localhost:1000", "http://localhost:1100"})
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

func generateAkubraConfig(numberOfShards int, numberOfStoragesPerShard int) *config.Config {
	akubraConfig := &config.Config{}
	akubraConfig.Storages = storagesConfig.StoragesMap{}
	akubraConfig.Shards = storagesConfig.ShardsMap{}

	weightPerShard := float64(1) / float64(numberOfStoragesPerShard)

	var shards []regionsConfig.Policy
	for shardNum := 0; shardNum < numberOfShards; shardNum++ {
		shards = append(shards, regionsConfig.Policy{Weight: weightPerShard, ShardName: fmt.Sprintf("test-%d", shardNum)})

		var storages storagesConfig.Storages
		for storageNum := 0; storageNum < numberOfStoragesPerShard; storageNum++ {
			endpoint, _ := url.Parse(fmt.Sprintf("http://localhost:%d", ((shardNum + 1)*1000) + (storageNum * 100)))

			akubraConfig.Storages[fmt.Sprintf("test-%d-%d", shardNum, storageNum)] = storagesConfig.Storage{
				Maintenance: false,
				Backend:     types.YAMLUrl{URL: endpoint},
				Type:        "passthrough",
			}

			storages = append(storages, storagesConfig.StorageBreakerProperties{Name: fmt.Sprintf("test-%d-%d", shardNum, storageNum)})
		}

		akubraConfig.Shards[fmt.Sprintf("test-%d", shardNum)] = storagesConfig.Shard{
			Storages: storages,
		}

	}

	akubraConfig.ShardingPolicies = regionsConfig.ShardingPolicies{}
	akubraConfig.ShardingPolicies["test"] = regionsConfig.Policies{
		Domains: []string{"localhost"},
		Shards:  shards,
	}

	akubraConfig.Service = httpConfig.Service{
		Client: httpConfig.Client{Transports: transportConfig.Transports{{}}}}

	return akubraConfig
}
