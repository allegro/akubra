package discovery

import (
	"fmt"
	"net/url"
	"time"

	"github.com/hashicorp/consul/api"
	"golang.org/x/sync/syncmap"
)

// DefaultCacheInvalidationTimeout for updating instances
const (
	DefaultCacheInvalidationTimeout = 10
)

// IHealth interface
type IHealth interface {
	Service(service, tag string, passingOnly bool, q *api.QueryOptions) ([]*api.ServiceEntry, *api.QueryMeta, error)
}

// HealthWrapper for consul api.Health
type HealthWrapper struct {
	c *api.Health
}

// Service function wrapper
func (hw *HealthWrapper) Service(service, tag string, passingOnly bool, q *api.QueryOptions) ([]*api.ServiceEntry, *api.QueryMeta, error) {
	return hw.c.Service(service, tag, passingOnly, q)
}

// IClient interface
type IClient interface {
	Health() IHealth
}

// ClientWrapper for consul api.Client
type ClientWrapper struct {
	Client *api.Client
}

// Health function wrapper
func (c *ClientWrapper) Health() IHealth {
	return c.Client.Health()
}

// Services for discovery service
type Services struct {
	ConsulClient IClient
	Instances    *syncmap.Map
	CacheTTL     int64
}

// GetEndpoint by service name
func (s *Services) GetEndpoint(serviceName string) (url *url.URL, err error) {
	resolver := s.UpdateInstances(serviceName)
	if resolver != nil && len(resolver.endpoints) > 0 {
		url = resolver.getHealthyInstanceEndpoint()
	} else {
		err = fmt.Errorf("no registered or healtly instances for service: %s", serviceName)
	}
	return
}

// UpdateInstances get service instances from service discovery
func (s *Services) UpdateInstances(serviceName string) (resolver *Resolver) {
	value, instancesExists := s.Instances.Load(serviceName)
	if instancesExists {
		resolver = value.(*Resolver)
	} else {
		s.Instances = new(syncmap.Map)
		resolver = NewResolver(s.ConsulClient)
	}

	if !instancesExists || time.Now().Unix()-resolver.LastUpdateTimestamp >= s.CacheTTL {
		if instancesExists {
			if !resolver.tryLock() {
				return resolver
			}
		} else {
			resolver.lock.Lock()
		}
		entries := resolver.GetNodesFromConsul(serviceName)
		if len(entries) > 0 {
			resolver.prepareInstancesEndpoints(entries)
			s.Instances.Store(serviceName, resolver)
		}
		resolver.updateLastTimestamp()
		resolver.lock.Unlock()
	}

	return resolver
}

// NewServices constructor
func NewServices(consulClient IClient, cacheInvalidationTimeout int64) *Services {
	return &Services{
		consulClient,
		new(syncmap.Map),
		cacheInvalidationTimeout,
	}
}
