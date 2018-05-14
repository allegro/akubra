package service

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIntegrationShouldGetNodesFromConsul(t *testing.T) {
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
	resolver := NewResolver(consulAPIClient)

	results := resolver.GetNodesFromConsul(testServiceName)

	assert.Equal(t, expectedServiceID+"1", results[0].Service.ID)
	assert.Equal(t, testServiceName, results[0].Service.Service)
	assert.Equal(t, expectedServiceID+"2", results[1].Service.ID)
	assert.Equal(t, testServiceName, results[1].Service.Service)
}
