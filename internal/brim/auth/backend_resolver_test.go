package auth

import (
	"fmt"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"

	"gopkg.in/yaml.v2"

	akubraconfig "github.com/allegro/akubra/internal/akubra/config"
	brimconfig "github.com/allegro/akubra/internal/brim/config"
)

const akubraConf = `
Service:
  Server:
    Listen: ":8080"
    TechnicalEndpointListen: ":8071"
  Client:
    AdditionalRequestHeaders:
      'Cache-Control': "public, s-maxage=600, max-age=600"
    AdditionalResponseHeaders:
      'Access-Control-Allow-Origin': "*"
      'Access-Control-Allow-Credentials': "true"
      'Access-Control-Allow-Methods': "GET, POST, OPTIONS"
    Transports:
      -
        Name: DefaultTransport
        Rules:
        Properties:
          MaxIdleConns: 300
          MaxIdleConnsPerHost: 600
          IdleConnTimeout: 0s
          ResponseHeaderTimeout: 5s

Storages:
  b1:
    Backend: http://b1:7480
    Type: passthrough
  b2:
    Backend: http://b2:7480
    Type: passthrough
  b3:
    Backend: http://b3:7480
    Type: passthrough

Shards:
  c1:
    Storages:
    - Name: b1
    - Name: b2
  c2:
    Storages:
    - Name: b3

ShardingPolicies:
  default:
    Shards: # to się powinno nazywać sharding
    - ShardName: c1
      Weight: 1
    - ShardName: c2
      Weight: 1
    Domains:
    - domain1
    Default: true

CredentialsStore:
  default:
    Endpoint: "http://localhost:8090"
    AuthRefreshInterval: 10s
`

const brimConf = `admins:
  b1:
  - adminaccesskey: "b1a"
    adminsecretkey: "b1s"
    adminprefix: "admin"
    endpoint: "http://b1:7480"
  b2:
  - adminaccesskey: "b2a"
    adminsecretkey: "b2s"
    adminprefix: "admin"
    endpoint: "http://b2:7480"
  b3:
  - adminaccesskey: "b3a"
    adminsecretkey: "b3s"
    adminprefix: "admin"
    endpoint: "http://b3:7480"
`

func parseConfs(akubraConf, brimConf string) (akubraconfig.Config, brimconfig.BrimConf) {
	ac := akubraconfig.YamlConfig{}
	err := yaml.Unmarshal([]byte(akubraConf), &ac)
	if err != nil {
		panic(err)
	}
	bc := brimconfig.BrimConf{}
	err = yaml.Unmarshal([]byte(brimConf), &bc)
	if err != nil {
		panic(err)
	}
	return akubraconfig.Config{YamlConfig: ac}, bc
}

func TestBackendDiscovery(t *testing.T) {
	cases := []struct {
		host     string
		key      string
		expected struct {
			endpoint string
			hasErr   bool
		}
	}{
		{host: "http://b1:7480", key: "some/key", expected: struct {
			endpoint string
			hasErr   bool
		}{endpoint: "b1", hasErr: false}},
		{host: "http://domain1", key: "some/key", expected: struct {
			endpoint string
			hasErr   bool
		}{endpoint: "b1", hasErr: false}},
		{host: "http://domain1", key: "some2/key2w3", expected: struct {
			endpoint string
			hasErr   bool
		}{endpoint: "b3", hasErr: false}},
		{host: "http://notinconfiguration", key: "some/key", expected: struct {
			endpoint string
			hasErr   bool
		}{endpoint: "", hasErr: true}},
	}
	ac, _ := parseConfs(akubraConf, brimConf)
	lookup := newLookup(&ac)
	for _, tc := range cases {
		backendName, ok := lookup.matchAkubraBackendName(tc.host, tc.key)
		if !tc.expected.hasErr {
			require.True(t, ok, fmt.Sprintf("TC %#v got %q", tc, backendName))
			require.Equal(t, backendName, tc.expected.endpoint)
		} else {
			require.False(t, ok)
		}
	}
}

func TestAdminCredsDiscovery(t *testing.T) {
	cases := []struct {
		host     string
		key      string
		expected struct {
			endpoint string
			hasErr   bool
		}
	}{
		{host: "http://domain1", key: "some/key", expected: struct {
			endpoint string
			hasErr   bool
		}{endpoint: "b1", hasErr: false}},
		{host: "http://domain1", key: "some2/key2w3", expected: struct {
			endpoint string
			hasErr   bool
		}{endpoint: "b3", hasErr: false}},
		{host: "http://b1", key: "some/key", expected: struct {
			endpoint string
			hasErr   bool
		}{endpoint: "b1", hasErr: false}},
		{host: "http://notinconfiguration", key: "some/key", expected: struct {
			endpoint string
			hasErr   bool
		}{endpoint: "", hasErr: true}},
	}
	ac, bc := parseConfs(akubraConf, brimConf)
	bs := NewConfigBasedBackendResolver(&ac, &bc)
	for _, tc := range cases {
		conf, err := bs.detectAdminCreds(tc.host, tc.key)
		if !tc.expected.hasErr {
			require.NoError(t, err)
			url, parseErr := url.Parse(conf.Endpoint)
			require.NoError(t, parseErr)
			require.Equal(t, url.Hostname(), tc.expected.endpoint)
		} else {
			require.Error(t, err)
		}
	}
}
