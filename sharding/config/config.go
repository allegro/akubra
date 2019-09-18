package config

import (
	"github.com/allegro/akubra/types"
)


// MultiClusterConfig defines region settings for multicluster
type MultiClusterConfig struct {
	// Domain name
	Cluster string `yaml:"Domain"`
	// Domain weight
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
