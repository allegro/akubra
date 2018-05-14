package service

import (
	"fmt"
	"net/url"
	"time"

	"github.com/hashicorp/consul/api"
	"golang.org/x/sync/syncmap"
)

const (
	updateInstancesCheckSeconds = 30
)

// Services for discovery service
type Services struct {
	ConsulClient *api.Client
	Instances    *syncmap.Map
}

// GetEndpoint by service name
func (s *Services) GetEndpoint(serviceName string) (url *url.URL, err error) {
	resolver := s.UpdateInstences(serviceName)
	if resolver != nil {
		url = resolver.prepareCurrentEndpoint()
	} else {
		err = fmt.Errorf("no registered or healtly instances for service: %s", serviceName)
	}
	return
}

// UpdateInstences get service instances form service discovery
func (s *Services) UpdateInstences(serviceName string) (resolver *Resolver) {
	value, exists := s.Instances.Load(serviceName)
	if exists {
		resolver = value.(*Resolver)
	}
	if s.Instances == nil || resolver == nil || !exists {
		s.Instances = new(syncmap.Map)
		resolver = NewResolver(s.ConsulClient)
		resolver.updateLastTimestamp()
	}

	if !exists || time.Now().Unix()-resolver.TTL >= updateInstancesCheckSeconds {
		if exists {
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
func New(consulClient *api.Client) *Services {
	return &Services{
		consulClient,
		new(syncmap.Map),
	}
}
