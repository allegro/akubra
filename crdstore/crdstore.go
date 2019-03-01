package crdstore

import (
	"fmt"
	"time"

	"errors"

	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/allegro/akubra/crdstore/config"
	"github.com/allegro/akubra/log"
	"golang.org/x/sync/syncmap"
)

const (
	keyPattern                   = "%s_____%s"
	requestOptionsRequestTimeout = 100 * time.Millisecond
	refreshTTLPercent            = 80 // Background refresh after refreshTTLPercent*TTL
)

// ErrCredentialsNotFound - Credential for given accessKey and backend haven't been found in yaml file
var ErrCredentialsNotFound = errors.New("credentials not found")

// CredentialStore instance
var credentialStores map[string]*CredentialsStore
var defaultCredentialStore *CredentialsStore

var credentialsStoresFactories = map[credentialsBackendType]credentialsBackendFactory{
	"Vault": &vaultCredsBackendFactory{},
}

type credentialsBackendType = string
type credentialsBackendFactory interface {
	create(crdStoreName string, backendProps map[string]string) (CredentialsBackend, error)
}

// CredentialsStores - gets a caches credentials from akubra-crdstore
type CredentialsStore struct {
	cache              *syncmap.Map
	TTL                time.Duration
	lock               sync.Mutex
	credentialsBackend CredentialsBackend
}

type CredentialsBackend interface {
	FetchCredentials(accessKey string, storageName string) (*CredentialsStoreData, error)
}

// GetInstance - Get crdstore instance for endpoint
func GetInstance(crdBackendName string) (instance *CredentialsStore, err error) {
	if instance, ok := credentialStores[crdBackendName]; ok {
		return instance, nil
	}
	return nil, fmt.Errorf("error credentialStore `%s` is not defined", crdBackendName)
}

// InitializeCredentialsStores - Constructor for CredentialsStores
func InitializeCredentialsStores(storeMap config.CredentialsStoreMap) {
	credentialStores = make(map[string]*CredentialsStore)

	for name, cfg := range storeMap {

		credsBackend, err := credentialsStoresFactories[cfg.Type].create(cfg.Properties)
		if err != nil {
			log.Fatalf("failed to initialize CredentialsStore '%s': %s", name, err)
		}
		credentialStores[name] = &CredentialsStore{
			cache:              new(syncmap.Map),
			TTL:                cfg.AuthRefreshInterval.Duration,
			credentialsBackend: credsBackend,
		}
	}
}

func (cs *CredentialsStore) prepareKey(accessKey, backend string) string {
	return fmt.Sprintf(keyPattern, accessKey, backend)
}

func (cs *CredentialsStore) updateCache(accessKey, backend, key string, csd *CredentialsStoreData, blocking bool) (credentials *CredentialsStoreData, err error) {

	switch blocking {
	case true:
		cs.lock.Lock()
	case false:
		if !cs.tryLock() {
			return csd, nil
		}
	}

	clientCreds, err := cs.credentialsBackend.FetchCredentials(accessKey, backend)
	if err != nil && err != ErrCredentialsNotFound {
		log.Printf("Error while updating cache for key `%s`: `%s`", key, err)
	}

	credentials = &CredentialsStoreData{
		AccessKey: clientCreds.AccessKey,
		SecretKey: clientCreds.SecretKey,
		EOL:       time.Now().Add(cs.TTL),
	}

	cs.cache.Store(key, credentials)
	cs.lock.Unlock()

	return credentials, nil
}

func (cs *CredentialsStore) tryLock() bool {
	// #nosec
	return atomic.CompareAndSwapInt32((*int32)(unsafe.Pointer(&cs.lock)), 0, 1)
}

// Get - Gets key from cache or from akubra-crdstore if TTL has expired
func (cs *CredentialsStore) Get(accessKey, backend string) (csd *CredentialsStoreData, err error) {
	key := cs.prepareKey(accessKey, backend)

	if value, credsPresentInCache := cs.cache.Load(key); credsPresentInCache {
		csd = value.(*CredentialsStoreData)
	}

	refreshTimeoutDuration := cs.TTL / 100 * (100 - refreshTTLPercent)
	switch {
	case csd == nil:
		return cs.updateCache(accessKey, backend, key, csd, true)
	case time.Now().After(csd.EOL):
		return cs.updateCache(accessKey, backend, key, csd, false)
	case time.Now().Add(refreshTimeoutDuration).After(csd.EOL):
		go func() {
			_, err = cs.updateCache(accessKey, backend, key, csd, false)
		}()
	}

	return
}