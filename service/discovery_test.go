package service

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegrationShouldNotGetEndpointWhereNoRegisteredInstances(t *testing.T) {
	testServices := make([]serviceTestEntry, 0)

	config, server := createTestConsulServer(t)
	defer stopConsul(server)
	consulAPIClient, entries := startConsulAgentAndRegisterService(t, config, &testServices)
	defer stopLoopbackServers(entries)
	discoveryServices := New(consulAPIClient, DefaultCacheInvalidationTimeout)

	_, err := discoveryServices.GetEndpoint(testServiceName)

	require.Error(t, err)
}

func TestIntegrationShouldGetEndpointFromTwoServiceInstances(t *testing.T) {
	expectedServiceID := "ID_"

	testServices := []serviceTestEntry{
		{
			id:   expectedServiceID + "1",
			name: testServiceName,
		},
		{
			id:   expectedServiceID + "2",
			name: testServiceName,
		},
	}

	config, server := createTestConsulServer(t)
	defer stopConsul(server)
	consulAPIClient, entries := startConsulAgentAndRegisterService(t, config, &testServices)
	defer stopLoopbackServers(entries)
	discoveryServices := New(consulAPIClient, DefaultCacheInvalidationTimeout)

	url, err := discoveryServices.GetEndpoint(testServiceName)

	require.NoError(t, err)

	anyHost := url.Host == entries[0].host+":"+entries[0].port || url.Host == entries[1].host+":"+entries[1].port

	assert.Equal(t, "http", url.Scheme)
	assert.True(t, anyHost)
}

func TestIntegrationShouldGetEndpointFromManyInstancesWithShortRevalidationTimeout(t *testing.T) {
	expectedServiceID := "ID_"
	var cacheInvalidationTimeout int64 = 1
	var testServices []serviceTestEntry

	for instancesCount := 20; instancesCount >= 0; instancesCount-- {
		testServices = append(testServices, serviceTestEntry{
			id: fmt.Sprintf("%s%d", expectedServiceID, instancesCount), name: testServiceName,
		})
	}

	config, server := createTestConsulServer(t)
	defer stopConsul(server)
	consulAPIClient, entries := startConsulAgentAndRegisterService(t, config, &testServices)
	defer stopLoopbackServers(entries)
	discoveryServices := New(consulAPIClient, cacheInvalidationTimeout)

	for testIterations := 100; testIterations >= 0; testIterations-- {
		url, err := discoveryServices.GetEndpoint(testServiceName)
		assert.Equal(t, "http", url.Scheme)
		require.NoError(t, err)
	}
}
