package discovery

import (
	"net"
	"strconv"
	"testing"

	"github.com/allegro/mesos-executor/xnet/xnettest"
	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/testutil"
	"github.com/stretchr/testify/require"
)

const testServiceName = "my-service-name"

type serviceTestEntry struct {
	id, name, host, port string
	listener             net.Listener
}

func createTestConsulServer(t *testing.T) (config *api.Config, server *testutil.TestServer) {
	server, err := testutil.NewTestServer()
	if err != nil {
		t.Fatal(err)
	}

	config = api.DefaultConfig()
	config.Address = server.HTTPAddr
	return config, server
}

func stopConsul(server *testutil.TestServer) {
	_ = server.Stop()
}

func stopLoopbackServers(testEntries []serviceTestEntry) {
	for _, entry := range testEntries {
		entry.listener.Close()
	}
}

func startConsulAgentAndRegisterService(t *testing.T, config *api.Config, testEntries *[]serviceTestEntry) (consulAPIClient *api.Client, entries []serviceTestEntry) {
	consulAPIClient, err := api.NewClient(config)
	require.NoError(t, err)

	if len(*testEntries) > 0 {
		agent := consulAPIClient.Agent()
		for _, entry := range *testEntries {
			// create listener acting as a service
			listener, _, err := xnettest.LoopbackServer("tcp4")
			require.NoError(t, err)

			host, portString, err := net.SplitHostPort(listener.Addr().String())
			require.NoError(t, err)
			port, _ := strconv.Atoi(portString)

			// register service in consul
			err = agent.ServiceRegister(&api.AgentServiceRegistration{
				ID:      entry.id,
				Name:    entry.name,
				Port:    port,
				Address: host,
			})
			entries = append(entries, serviceTestEntry{id: entry.id, name: entry.name, host: host, port: portString, listener: listener})
			require.NoError(t, err)
		}
	}

	return consulAPIClient, entries
}
