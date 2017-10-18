package config

import (
	"fmt"

	"github.com/allegro/akubra/types"
)

// MultiClusterConfig defines region settings for multicluster
type MultiClusterConfig struct {
	// Cluster name
	Cluster string `yaml:"Cluster"`
	// Cluster weight
	Weight float64 `yaml:"Weight"`
}

// RegionConfig region configuration
type RegionConfig struct {
	// Multi cluster config
	Clusters []MultiClusterConfig `yaml:"Clusters"`
	// Domains used for region matching
	Domains []string `yaml:"Domains"`
	// Default region will be applied if Host header would not match any other region
	Default bool `yaml:"Default,omitempty"`
}

// YAMLUrl is yaml deserializing wrapper for net/url.URL
type YAMLUrl = types.YAMLUrl

// SyncLogMethod type fields in yaml configuration will parse list of HTTP methods
type SyncLogMethod struct {
	Method string
}

// UnmarshalYAML for SyncLogMethod
func (slm *SyncLogMethod) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}
	method := fmt.Sprintf("%v", s)
	switch method {
	case "GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS":
		break
	default:
		return fmt.Errorf("Sync log method should be one from [GET, POST, DELETE, HEAD, OPTIONS] - got %q", s)
	}
	slm.Method = method
	return nil
}

// HumanSizeUnits is yaml deserializer
type HumanSizeUnits = types.HumanSizeUnits
