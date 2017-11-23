package config

import (
	"github.com/allegro/akubra/metrics"
	"github.com/allegro/akubra/types"
)

// CredentialsStore configuration
type CredentialsStore struct {
	// Endpoint url points ObjectStorage API url
	Endpoint types.YAMLUrl `yaml:"Endpoint"`
	// AuthRefreshInterval defines how often CredentialsStore cache will lookup for value changes
	AuthRefreshInterval metrics.Interval `yaml:"AuthRefreshInterval"`
}

// CredentialsStoreMap - map of credentialsStores configurations
type CredentialsStoreMap map[string]CredentialsStore
