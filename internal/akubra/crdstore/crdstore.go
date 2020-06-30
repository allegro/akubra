package crdstore

import (
	"fmt"
	"time"

	"errors"

	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/allegro/akubra/internal/akubra/crdstore/config"
	"github.com/allegro/akubra/internal/akubra/log"
	"golang.org/x/sync/syncmap"
)

const (
	keyPattern        = "%s_____%s"
	refreshTTLPercent = 80 // Background refresh after refreshTTLPercent*TTL
)

// ErrCredentialsNotFound - Credential for given accessKey and backend haven't been found in yaml file
var ErrCredentialsNotFound = errors.New("credentials not found")

//DefaultCredentialsStoreName holds the default CredentialsStore name
var DefaultCredentialsStoreName string
var credentialsStores map[string]*CredentialsStore
var credentialsStoresFactories = map[credentialsBackendType]credentialsBackendFactory{
	"Vault": &vaultCredsBackendFactory{},
	"BalancedVault": &balancedVaultClientFactory{},
}

type credentialsBackendType = string
type credentialsBackendFactory interface {
	create(crdStoreName string, backendProps map[string]string) (CredentialsBackend, error)
}

// CredentialsStore - gets a caches credentials from akubra-crdstore
type CredentialsStore struct {
	cache              *syncmap.Map
	TTL                time.Duration
	lock               sync.Mutex
	credentialsBackend CredentialsBackend
}

//CredentialsBackend is responsible for holding storage credentials
type CredentialsBackend interface {
	FetchCredentials(accessKey string, storageName string) (*CredentialsStoreData, error)
}

// GetInstance - Get crdstore instance by store's name
func GetInstance(crdBackendName string) (instance *CredentialsStore, err error) {
	if instance, ok := credentialsStores[crdBackendName]; ok {
		return instance, nil
	}
	return nil, fmt.Errorf("error credentialsStore `%s` is not defined", crdBackendName)
}

// InitializeCredentialsStores - Constructor for CredentialsStores
func InitializeCredentialsStores(storeMap config.CredentialsStoreMap) {
	credentialsStores = make(map[string]*CredentialsStore)

	for name, cfg := range storeMap {

		if _, supported := credentialsStoresFactories[cfg.Type]; !supported {
			log.Fatalf("unsupported CredentialsStore '%s'", cfg.Type)
		}
		credsBackend, err := credentialsStoresFactories[cfg.Type].create(name, cfg.Properties)
		if err != nil {
			log.Fatalf("failed to initialize CredentialsStore '%s': %s, %#v", name, err, credentialsStoresFactories[cfg.Type])
		}
		if cfg.Default {
			DefaultCredentialsStoreName = name
		}
		credentialsStores[name] = &CredentialsStore{
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
	credentials, err = cs.credentialsBackend.FetchCredentials(accessKey, backend)
	switch {
	case err == nil:
		credentials.err = nil
	case err == ErrCredentialsNotFound:
		credentials = &CredentialsStoreData{EOL: time.Now().Add(cs.TTL), err: ErrCredentialsNotFound}
	default:
		if csd == nil {
			credentials = &CredentialsStoreData{EOL: time.Now().Add(cs.TTL), err: err}
		} else {
			credentials = &CredentialsStoreData{}
			*credentials = *csd
		}
		credentials.err = err
		log.Printf("Error while updating cache for key `%s`: `%s`", key, err)
	}
	credentials.EOL = time.Now().Add(cs.TTL)
	cs.cache.Store(key, credentials)
	defer cs.lock.Unlock()
	if credentials.AccessKey == "" {
		return nil, credentials.err
	}
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
	case csd == nil || csd.AccessKey == "":
		return cs.updateCache(accessKey, backend, key, csd, true)
	case time.Now().After(csd.EOL):
		return cs.updateCache(accessKey, backend, key, csd, false)
	case time.Now().Add(refreshTimeoutDuration).After(csd.EOL):
		go func() {
			_, err := cs.updateCache(accessKey, backend, key, csd, false)
			if err != nil {
				log.Debugln("Failed to update cache %q", err)
			}
		}()
	}

	return
}
