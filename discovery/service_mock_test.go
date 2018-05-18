package discovery

import (
	"fmt"
	"github.com/golib/assert"
	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/mock"
	"testing"
	"time"
)

type ConsulClientWrapperMock struct {
	mock.Mock
}

// Health function mock
func (hw *ConsulClientWrapperMock) Health() IHealth {
	args := hw.Called()
	if health, ok := args.Get(0).(IHealth); ok {
		return health
	}
	return nil
}

type HealthWrapperMock struct {
	mock.Mock
}

// Service function mock
func (hw *HealthWrapperMock) Service(service, tag string, passingOnly bool, q *api.QueryOptions) ([]*api.ServiceEntry, *api.QueryMeta, error) {
	args := hw.Called()
	return args.Get(0).([]*api.ServiceEntry), args.Get(1).(*api.QueryMeta), args.Error(2)
}

func TestShouldGetEndpointWhenOneInstanceExists(t *testing.T) {
	host := "localhost"
	port := 12345
	expectedHost := fmt.Sprintf("%s:%d", host, port)

	consulClientMock := prepareConsulClientMock(host, port, 1)
	services := NewServices(consulClientMock, 1)

	instance, err := services.GetEndpoint("my-service")

	assert.NoError(t, err)
	assert.Equal(t, instance.Host, expectedHost)
}

func TestShouldGetEndpointWhenTwoInstancesExists(t *testing.T) {
	host := "localhost"
	port := 12345
	expectedHost1 := fmt.Sprintf("%s:%d", host, port)
	expectedHost2 := fmt.Sprintf("%s:%d", host, port+1)

	consulClientMock := prepareConsulClientMock(host, port, 2)
	services := NewServices(consulClientMock, 5)

	instance, err := services.GetEndpoint("my-service")

	assert.NoError(t, err)

	assert.True(t, instance.Host == expectedHost1 || instance.Host == expectedHost2)
}

func TestShouldGetEndpointReturnEndpointAfterTwiceCalling(t *testing.T) {
	host := "localhost"
	port := 12345
	expectedHost1 := fmt.Sprintf("%s:%d", host, port)
	expectedHost2 := fmt.Sprintf("%s:%d", host, port+1)

	consulClientMock := prepareConsulClientMock(host, port, 2)
	services := NewServices(consulClientMock, 1)

	instance, err := services.GetEndpoint("my-service")

	assert.NoError(t, err)
	assert.True(t, instance.Host == expectedHost1 || instance.Host == expectedHost2)

	time.Sleep(2 * time.Second)

	instance, err = services.GetEndpoint("my-service")

	assert.NoError(t, err)
	assert.True(t, instance.Host == expectedHost1 || instance.Host == expectedHost2)
}

func TestShouldNotGetEndpoint(t *testing.T) {
	consulClientMock := prepareConsulClientMock("", 0, 0)
	services := NewServices(consulClientMock, 10)

	_, err := services.GetEndpoint("my-service")

	assert.Error(t, err)
}

func prepareConsulClientMock(address string, port int, entitiesCount int) IClient {
	entries := make([]*api.ServiceEntry, 0)
	if entitiesCount > 0 {
		for cnt := 0; cnt < entitiesCount; cnt++ {
			entries = append(entries, &api.ServiceEntry{
				Service: &api.AgentService{
					Address: address,
					Port:    port + cnt,
				},
			})
		}
	}
	queryMeta := &api.QueryMeta{}
	var err error

	consulClientMock := &ConsulClientWrapperMock{}
	consulHealthMock := new(HealthWrapperMock)
	consulHealthMock.On("Service").Return(entries, queryMeta, err)
	consulClientMock.On("Health").Return(consulHealthMock)

	return consulClientMock
}
