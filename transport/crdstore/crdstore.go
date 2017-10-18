package crdstore

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"errors"

	"github.com/allegro/akubra/log"
	"github.com/levigross/grequests"
)

const (
	keyPattern                   = "%s_____%s"
	requestOptionsDialTimeout    = 50 * time.Millisecond
	requestOptionsRequestTimeout = 100 * time.Millisecond
)

// ErrCredentialsNotFound - Credential for given accessKey and storageType haven't been found in yaml file
var ErrCredentialsNotFound = errors.New("credentials not found")

// CdrStoreCache - Map with CdrStore cache
type CdrStoreCache map[string]CredentialsStoreData
type getHandler func(string, string, string) (*CredentialsStoreData, error)

// CredentialsStore - gets a caches credentials from akubra-crdstore
type CredentialsStore struct {
	endpoint string
	cache    CdrStoreCache
	TTL      time.Duration
	*sync.RWMutex
}

// NewCredentialsStore - Constructor for CredentialsStore
func NewCredentialsStore(endpoint string, TTL time.Duration) *CredentialsStore {
	return &CredentialsStore{
		endpoint: endpoint,
		cache:    make(CdrStoreCache),
		TTL:      TTL,
		RWMutex:  new(sync.RWMutex),
	}
}

func (cs *CredentialsStore) prepareKey(accessKey, storageType string) string {
	return fmt.Sprintf(keyPattern, accessKey, storageType)
}

// Get - Gets key from cache or from akubra-crdstore is TTL has expired
func (cs *CredentialsStore) Get(accessKey, storageType string) (csd *CredentialsStoreData, err error) {
	key := cs.prepareKey(accessKey, storageType)
	// Get from cache
	cs.RLock()
	if value, found := cs.cache[key]; found {
		csd = &value
	}
	cs.RUnlock()

	if csd != nil && csd.EOL.After(time.Now()) {
		return
	}

	// Update cache
	cs.Lock()
	newCsd, err := cs.GetFromService(cs.endpoint, accessKey, storageType)
	switch err {
	case nil:
		cs.cache[key] = *newCsd
		csd = newCsd
	case ErrCredentialsNotFound:
		delete(cs.cache, key)
	default:
		log.Printf("Error while updating cache for key `%s`: `%s`", key, err)
	}
	if csd != nil {
		csd.EOL = time.Now().Add(cs.TTL)
		err = nil
	}
	cs.Unlock()

	return
}

// GetFromService - Get Credential akubra-crdstore service
func (cs *CredentialsStore) GetFromService(endpoint, accessKey, storageType string) (csd *CredentialsStoreData, err error) {
	csd = &CredentialsStoreData{}
	ro := &grequests.RequestOptions{
		DialTimeout:    requestOptionsDialTimeout,
		RequestTimeout: requestOptionsRequestTimeout,
		RedirectLimit:  1,
		IsAjax:         false,
	}
	resp, err := grequests.Get(fmt.Sprintf(urlPattern, endpoint, accessKey, storageType), ro)
	switch {
	case err != nil:
		return csd, fmt.Errorf("unable to make request to credentials store service - err: %s", err)
	case resp.StatusCode == http.StatusNotFound:
		return nil, ErrCredentialsNotFound
	case resp.StatusCode != http.StatusOK:
		return csd, fmt.Errorf("unable to get credentials from store service - StatusCode: %d", resp.StatusCode)
	}

	credentials := resp.String()
	if len(credentials) == 0 {
		return csd, fmt.Errorf("got empty credentials from store service%s", "")
	}

	csd.Unmarshal(credentials)

	return
}
