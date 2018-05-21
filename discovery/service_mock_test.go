package discovery

import (
	"fmt"
	"testing"

	"net/url"

	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const testServiceName = "my-service"

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

	instance, err := services.GetEndpoint(testServiceName)

	require.NoError(t, err)
	require.Equal(t, instance.Host, expectedHost)
}

func TestShouldGetEndpointWhenTwoInstancesExist(t *testing.T) {
	host := "localhost"
	port := 12345
	expectedHost1 := fmt.Sprintf("%s:%d", host, port)
	expectedHost2 := fmt.Sprintf("%s:%d", host, port+1)

	consulClientMock := prepareConsulClientMock(host, port, 2)
	services := NewServices(consulClientMock, 5)

	instance, err := services.GetEndpoint(testServiceName)

	require.NoError(t, err)
	require.True(t, existAnyHostInInstance(instance, expectedHost1, expectedHost2))
}

func TestShouldReturnEndpointAfterDoubleCallingWithCacheRevalidation(t *testing.T) {
	host := "localhost"
	port := 12345
	expectedHost1 := fmt.Sprintf("%s:%d", host, port)
	expectedHost2 := fmt.Sprintf("%s:%d", host, port+1)

	consulClientMock := prepareConsulClientMock(host, port, 2)
	services := NewServices(consulClientMock, 0)

	instance, err := services.GetEndpoint(testServiceName)

	require.NoError(t, err)
	require.True(t, existAnyHostInInstance(instance, expectedHost1, expectedHost2))

	instance, err = services.GetEndpoint(testServiceName)

	require.NoError(t, err)
	require.True(t, existAnyHostInInstance(instance, expectedHost1, expectedHost2))
}

func TestShouldNotGetEndpoint(t *testing.T) {
	consulClientMock := prepareConsulClientMock("", 0, 0)
	services := NewServices(consulClientMock, 10)

	_, err := services.GetEndpoint(testServiceName)

	require.Error(t, err)
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

func existAnyHostInInstance(instance *url.URL, expectedHost1, expectedHost2 string) bool {
	return instance.Host == expectedHost1 || instance.Host == expectedHost2
}
