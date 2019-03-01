package config

import (
	"github.com/allegro/akubra/metrics"
)

// CredentialsStores configuration
type CredentialsStore struct {
	// Type specifies which implementation should be used when instating the CredentialStore
	Type string
	// Default tells if the CredentialsStore is a default one
	Default bool
	// Properties hols the properties needed to use the CredentialsStore
	Properties map[string]string
	// AuthRefreshInterval defines how often CredentialsStores cache will lookup for value changes
	AuthRefreshInterval metrics.Interval `yaml:"AuthRefreshInterval"`
}

// CredentialsStoreMap - map of credentialsStores configurations
type CredentialsStoreMap map[string]CredentialsStore
