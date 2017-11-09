package config

import (
	"github.com/allegro/akubra/metrics"
	"github.com/allegro/akubra/types"
)

// CredentialsStore - config for credential store
type CredentialsStore struct {
	Endpoint types.YAMLUrl    `yaml:"Endpoint"`
	CacheTTL metrics.Interval `yaml:"CacheTTL"`
}

// CredentialsStoreMap - map of credentialsStores configurations
type CredentialsStoreMap map[string]CredentialsStore
