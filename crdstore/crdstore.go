package crdstore

import (
	"fmt"
	"net/http"
	"time"

	"errors"

	"sync"
	"sync/atomic"
	"unsafe"

	"io/ioutil"

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
var instances map[string]*CredentialsStore

// CredentialsStore - gets a caches credentials from akubra-crdstore
type CredentialsStore struct {
	endpoint string
	cache    *syncmap.Map
	TTL      time.Duration
	lock     sync.Mutex
}

// GetInstance - Get crdstore instance for endpoint
func GetInstance(endpointName string) (instance *CredentialsStore, err error) {
	if instance, ok := instances[endpointName]; ok {
		return instance, nil
	}
	return nil, fmt.Errorf("error credentialStore `%s` is not defined", endpointName)
}

// InitializeCredentialsStore - Constructor for CredentialsStore
func InitializeCredentialsStore(storeMap config.CredentialsStoreMap) {
	instances = make(map[string]*CredentialsStore)
	for name, cfg := range storeMap {
		instances[name] = &CredentialsStore{
			endpoint: cfg.Endpoint.String(),
			cache:    new(syncmap.Map),
			TTL:      cfg.AuthRefreshInterval.Duration,
		}
	}
}

func (cs *CredentialsStore) prepareKey(accessKey, backend string) string {
	return fmt.Sprintf(keyPattern, accessKey, backend)
}

func (cs *CredentialsStore) updateCache(accessKey, backend, key string, csd *CredentialsStoreData, blocking bool) (newCsd *CredentialsStoreData, err error) {
	if !blocking {
		if !cs.tryLock() {
			return csd, nil
		}
	} else {
		cs.lock.Lock()
	}
	newCsd, err = cs.GetFromService(cs.endpoint, accessKey, backend)
	switch {
	case err == nil:
		newCsd.err = nil
	case err == ErrCredentialsNotFound:
		newCsd = &CredentialsStoreData{EOL: time.Now().Add(cs.TTL), err: ErrCredentialsNotFound}
	default:
		if csd == nil {
			newCsd = &CredentialsStoreData{EOL: time.Now().Add(cs.TTL), err: err}
		} else {
			*newCsd = *csd
		}
		newCsd.err = err
		log.Printf("Error while updating cache for key `%s`: `%s`", key, err)
	}
	newCsd.EOL = time.Now().Add(cs.TTL)
	cs.cache.Store(key, newCsd)
	cs.lock.Unlock()
	if newCsd.AccessKey == "" {
		return nil, newCsd.err
	}
	return newCsd, nil
}

func (cs *CredentialsStore) tryLock() bool {
	// #nosec
	return atomic.CompareAndSwapInt32((*int32)(unsafe.Pointer(&cs.lock)), 0, 1)
}

// Get - Gets key from cache or from akubra-crdstore if TTL has expired
func (cs *CredentialsStore) Get(accessKey, backend string) (csd *CredentialsStoreData, err error) {
	key := cs.prepareKey(accessKey, backend)

	if value, ok := cs.cache.Load(key); ok {
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

// GetFromService - Get Credential akubra-crdstore service
func (cs *CredentialsStore) GetFromService(endpoint, accessKey, backend string) (csd *CredentialsStoreData, err error) {
	csd = &CredentialsStoreData{}
	client := http.Client{
		Timeout: requestOptionsRequestTimeout,
	}
	resp, err := client.Get(fmt.Sprintf(urlPattern, endpoint, accessKey, backend))
	switch {
	case err != nil:
		return csd, fmt.Errorf("unable to make request to credentials store service - err: %s", err)
	case resp.StatusCode == http.StatusNotFound:
		return nil, ErrCredentialsNotFound
	case resp.StatusCode != http.StatusOK:
		return csd, fmt.Errorf("unable to get credentials from store service - StatusCode: %d (backend: `%s`, endpoint: `%s`", resp.StatusCode, backend, endpoint)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			log.Printf("Cannot close request body: %q\n", closeErr)
		}
	}()

	credentials, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return csd, fmt.Errorf("unable to read response body from credentials store service - err: %s", err)
	}

	if len(credentials) == 0 {
		return csd, fmt.Errorf("got empty credentials from store service%s", "")
	}

	err = csd.Unmarshal(credentials)

	return
}
