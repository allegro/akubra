package config

import (
	"time"

	"github.com/allegro/akubra/types"
)

// CredentialsStore - config for credential store
type CredentialsStore struct {
	Endpoint types.YAMLUrl `yaml:"Endpoint"`
	TTL      time.Duration `yaml:"TTL"`
}

// CredentialsStoreMap - map of credentialsStores configurations
type CredentialsStoreMap map[string]CredentialsStore
