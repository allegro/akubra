package service

import (
	"fmt"
	"net/url"
	"time"

	"github.com/hashicorp/consul/api"
	"golang.org/x/sync/syncmap"
)

// DefaultCacheInvalidationTimeout for updating instances
const (
	DefaultCacheInvalidationTimeout = 30
)

// Services for discovery service
type Services struct {
	ConsulClient             *api.Client
	Instances                *syncmap.Map
	CacheInvalidationTimeout int64
}

// GetEndpoint by service name
func (s *Services) GetEndpoint(serviceName string) (url *url.URL, err error) {
	resolver := s.UpdateInstences(serviceName)
	if resolver != nil && len(resolver.endpoints) > 0 {
		url = resolver.getHealthyInstanceEndpoint()
	} else {
		err = fmt.Errorf("no registered or healtly instances for service: %s", serviceName)
	}
	return
}

// UpdateInstences get service instances from service discovery
func (s *Services) UpdateInstences(serviceName string) (resolver *Resolver) {
	value, instancesExists := s.Instances.Load(serviceName)
	if instancesExists {
		resolver = value.(*Resolver)
	} else {
		s.Instances = new(syncmap.Map)
		resolver = NewResolver(s.ConsulClient)
		resolver.updateLastTimestamp()
	}

	if !instancesExists || time.Now().Unix()-resolver.TTL >= s.CacheInvalidationTimeout {
		if instancesExists {
			if !resolver.tryLock() {
				return resolver
			}
		} else {
			resolver.lock.Lock()
		}
		entries := resolver.GetNodesFromConsul(serviceName)

		resolver.endpoints = make([]*url.URL, 0)
		for _, entry := range entries {
			resolver.endpoints = append(resolver.endpoints, serviceEntryToURL(entry))
		}
		resolver.updateLastTimestamp()
		s.Instances.Store(serviceName, resolver)
		resolver.lock.Unlock()
	}

	return resolver
}

// New Services constructor
func New(consulClient *api.Client, cacheInvalidationTimeout int64) *Services {
	return &Services{
		consulClient,
		new(syncmap.Map),
		cacheInvalidationTimeout,
	}
}
