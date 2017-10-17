package crdstore

import (
	"time"

	"fmt"

	"sync"

	"github.com/allegro/akubra/log"
	"github.com/wunderlist/ttlcache"
)

const (
	KeyPattern                   = "%s_____%s"
	RequestOptionsDialTimeout    = 50 * time.Millisecond
	RequestOptionsRequestTimeout = 100 * time.Millisecond
)

// CdrStoreServiceAPI interface
type CdrStoreServiceAPI interface {
	GetFromService(endpoint, accessKey, storageType string) (csd *CredentialsStoreData, err error)
}

type CdrStoreBackup map[string]CredentialsStoreData

type CredentialsStore struct {
	endpoint       string
	cache          *ttlcache.Cache
	backup         CdrStoreBackup
	lock           sync.RWMutex
	defaultService CdrStoreServiceAPI
}

func NewCredentialsStore(endpoint string, duration time.Duration) *CredentialsStore {
	return &CredentialsStore{
		endpoint:       endpoint,
		cache:          ttlcache.NewCache(duration),
		backup:         make(CdrStoreBackup),
		defaultService: nil,
	}
}

func (cs *CredentialsStore) prepareKey(accessKey, storageType string) string {
	return fmt.Sprintf(KeyPattern, accessKey, storageType)
}

func (cs *CredentialsStore) Get(accessKey, storageType string) (csd *CredentialsStoreData, found bool) {
	key := cs.prepareKey(accessKey, storageType)
	credentials, found := cs.cache.Get(key)
	if !found {
		var err error
		if cs.defaultService != nil {
			csd, err = cs.defaultService.GetFromService(cs.endpoint, accessKey, storageType)
		} else {
			csd, err = csd.GetFromService(cs.endpoint, accessKey, storageType)
		}
		if err != nil {
			log.Println(err)
			//csd, err = cs.getFromBackup(key)
			//TODO: extract value
			//if err != nil {
			//	log.Fatalln(err)
			//	found = false
			//} else {
			//	found = true
			//}
			return csd, false
		} else {
			return csd, true
		}
	} else {
		csd.Unmarshal(credentials)
	}
	return csd, found
}
