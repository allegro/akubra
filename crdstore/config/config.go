package config

import (
	"github.com/allegro/akubra/metrics"
)

// CredentialsStore configuration
type CredentialsStore struct {
	// Type specifies which implementation should be used when instating the CredentialsStore
	Type string `yaml:"Type"`
	// Default tells if the CredentialsStore is a default one
	Default bool `yaml:"Default"`
	// Properties hols the properties needed to use the CredentialsStore
	Properties map[string]string `yaml:"Properties"`
	// AuthRefreshInterval defines how often CredentialsStores cache will lookup for value changes
	AuthRefreshInterval metrics.Interval `yaml:"AuthRefreshInterval"`
}

// CredentialsStoreMap - map of credentialsStores configurations
type CredentialsStoreMap map[string]CredentialsStore
