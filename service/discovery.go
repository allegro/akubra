package service

import (
	"fmt"
	"net/url"
	"time"

	"github.com/hashicorp/consul/api"
)

const (
	updateInstancesCheckSeconds = 3
)

// Services for discovery service
type Services struct {
	ConsulClient *api.Client
	Logger       func(format string, args ...interface{})
	Instances    map[string]*Resolver
	// TODO: Instances    *syncmap.Map
}

// GetEndpoint by service name
func (s *Services) GetEndpoint(serviceName string) (*url.URL, error) {
	s.GetInstances(serviceName, s.Instances == nil)
	_, exists := s.Instances[serviceName]
	if !exists || len(s.Instances[serviceName].endpoints) == 0 {
		// TODO: quasi-backoff ???
		//time.Sleep(1 * time.Second)
		return nil, fmt.Errorf("no registered or healtly instances for service: %s", serviceName)
	}

	return s.Instances[serviceName].prepareCurrentEndpoint(), nil
}

// GetInstances get service instances form service discovery
func (s *Services) GetInstances(serviceName string, blocking bool) (instances []*url.URL) {
	if s.Instances == nil {
		s.Logger("GetInstances(): init instances")
		s.Instances = make(map[string]*Resolver, 0)
	}
	//s.Logger("try to get instances for serviceName: %s", serviceName)
	resolver, exists := s.Instances[serviceName]
	if !exists {
		s.Logger("GetInstances(): try to get instances - !exists")
		resolver = NewResolver(s.ConsulClient)
		resolver.updateLastTimestamp()
	}
	if blocking || time.Now().Unix()-resolver.TTL >= updateInstancesCheckSeconds {
		s.Logger("LOCK:")
		if !blocking {
			s.Logger(" - !blocking")
			if !resolver.tryLock() {
				s.Logger(" - !resolver.tryLock()")
				return resolver.endpoints
			}
		} else {
			s.Logger(" - blocking - resolver.lock.Lock()")
			resolver.lock.Lock()
		}
		entries := resolver.GetNodesFromConsul(serviceName)
		s.Logger("GetInstances(): resolver.GetNodesFromConsul - serviceName: %q - entries: %q", serviceName, entries)

		resolver.endpoints = make([]*url.URL, 0)
		for _, entry := range entries {
			resolver.endpoints = append(resolver.endpoints, serviceEntryToURL(entry))
		}
		resolver.updateLastTimestamp()
		s.Instances[serviceName] = resolver
		resolver.lock.Unlock()
		s.Logger("UNLOCK.\n\n")
	}

	return resolver.endpoints
}

func New(consulClient *api.Client, logger func(format string, args ...interface{})) *Services {
	return &Services{
		consulClient,
		logger,
		nil,
	}
}
