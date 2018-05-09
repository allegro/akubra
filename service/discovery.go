package service

import (
	"fmt"
	"math/rand"
	"net/url"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/pmylund/go-cache"
)

const (
	defaultExpirationDuration = time.Second * 5
	cleanupIntervalDuration   = time.Second * 30
)

// Resolver for discovery service
type Resolver struct {
	cache     *cache.Cache
	client    *api.Client
	generator *rand.Rand
}

// GetNodes get service nodes form service discovery
func (resolver *Resolver) GetNodes(service string) (entries []*api.ServiceEntry) {
	if value, exists := resolver.cache.Get(service); exists {
		return value.([]*api.ServiceEntry)
	}

	nodes, _, error := resolver.client.Health().Service(service, "", true, &api.QueryOptions{
		AllowStale:        true,
		RequireConsistent: false,
	})

	if error == nil {
		entries = nodes
	} else {
		entries = make([]*api.ServiceEntry, 0)
	}

	defer resolver.cache.Set(service, entries, cache.DefaultExpiration)

	return
}

// GetRandomNode from service nodes list
func (resolver *Resolver) GetRandomNode(service string) (*url.URL, error) {
	nodes := resolver.GetNodes(service)
	if len(nodes) == 0 {
		return nil, fmt.Errorf("there are no healthy nodes for service: %s", service)
	}

	return serviceEntryToURL(nodes[resolver.generator.Intn(len(nodes))]), nil
}

func serviceEntryToURL(entry *api.ServiceEntry) *url.URL {
	url := &url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%d", entry.Service.Address, entry.Service.Port),
	}

	return url
}

// New resolver for discovery client
func New(client *api.Client) *Resolver {
	return &Resolver{
		client:    client,
		cache:     cache.New(defaultExpirationDuration, cleanupIntervalDuration),
		generator: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}
