package service

import (
	"fmt"
	"math/rand"
	"net/url"
	"time"

	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/hashicorp/consul/api"
)

// Resolver for discovery service
type Resolver struct {
	consulClient    *api.Client
	endpoints       []*url.URL
	currentEndpoint *url.URL
	generator       *rand.Rand
	TTL             int64
	lock            sync.Mutex
}

// GetNodesFromConsul get service nodes form service discovery
func (r *Resolver) GetNodesFromConsul(service string) (entries []*api.ServiceEntry) {
	entries = make([]*api.ServiceEntry, 0)
	nodes, _, error := r.consulClient.Health().Service(service, "", true, &api.QueryOptions{
		AllowStale:        true,
		RequireConsistent: false,
	})
	if error == nil {
		entries = nodes
	}
	return
}

func (r *Resolver) getHealthyInstanceEndpoint() (currentEndpoint *url.URL) {
	currentEndpoint = r.endpoints[r.generator.Intn(len(r.endpoints))]
	r.currentEndpoint = currentEndpoint
	return
}

func (r *Resolver) updateLastTimestamp() {
	r.TTL = time.Now().Unix()
}

func (r *Resolver) tryLock() bool {
	// #nosec
	return atomic.CompareAndSwapInt32((*int32)(unsafe.Pointer(&r.lock)), 0, 1)
}

// serviceEntryToURL converting service endpoint from consul api.ServiceEntry to URL
func serviceEntryToURL(entry *api.ServiceEntry) *url.URL {
	url := &url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%d", entry.Service.Address, entry.Service.Port),
	}

	return url
}

// NewResolver for discovery client
func NewResolver(consulClient *api.Client) *Resolver {
	return &Resolver{
		consulClient: consulClient,
		generator:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}
