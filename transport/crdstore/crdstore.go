package crdstore

import (
	"fmt"
	"net/http"
	"time"

	"errors"

	"github.com/allegro/akubra/log"
	"github.com/levigross/grequests"
	"golang.org/x/sync/syncmap"
	"sync"
	"sync/atomic"
	"unsafe"
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
	cache    *syncmap.Map
	TTL      time.Duration
	lock     sync.Mutex
}

// NewCredentialsStore - Constructor for CredentialsStore
func NewCredentialsStore(endpoint string, TTL time.Duration) *CredentialsStore {
	return &CredentialsStore{
		endpoint: endpoint,
		cache:    new(syncmap.Map),
		TTL:      TTL,
	}
}

func (cs *CredentialsStore) prepareKey(accessKey, storageType string) string {
	return fmt.Sprintf(keyPattern, accessKey, storageType)
}

// Get - Gets key from cache or from akubra-crdstore if TTL has expired
func (cs *CredentialsStore) Get(accessKey, storageType string) (csd *CredentialsStoreData, err error) {
	key := cs.prepareKey(accessKey, storageType)

	// Get from cache
	if value, ok := cs.cache.Load(key); ok {
		if csd, ok = value.(*CredentialsStoreData); ok {
			err = csd.err
		}
	}

	if csd != nil && (time.Now().Before(csd.EOL) || atomic.LoadInt32((*int32)(unsafe.Pointer(&cs.lock))) != 0) {
		return
	}

	// Update cache
	cs.lock.Lock()
	newCsd, err := cs.GetFromService(cs.endpoint, accessKey, storageType)
	switch {
	case err == nil:
		newCsd.err = nil
	case err == ErrCredentialsNotFound:
		newCsd = &CredentialsStoreData{EOL: time.Now().Add(cs.TTL), err: ErrCredentialsNotFound}
	default:
		if csd == nil {
			newCsd = &CredentialsStoreData{EOL: time.Now().Add(cs.TTL), err: ErrCredentialsNotFound}
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
	} else {
		return newCsd, nil
	}
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

	err = csd.Unmarshal(credentials)

	return
}
