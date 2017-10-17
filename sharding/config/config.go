package config

import (
	"fmt"
	"net/url"
)

// ClusterConfig defines cluster configuration
type ClusterConfig struct {
	// Backends should contain s3 backend urls
	Backends []YAMLUrl `yaml:"Backends"`
}

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

// YAMLUrl type fields in yaml configuration will parse urls
type YAMLUrl struct {
	*url.URL
}

// SyncLogMethod type fields in yaml configuration will parse list of HTTP methods
type SyncLogMethod struct {
	Method string
}

// UnmarshalYAML for YAMLUrl
func (yurl *YAMLUrl) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}
	url, err := url.Parse(s)
	if url.Host == "" {
		return fmt.Errorf("url should match proto://host[:port]/path scheme - got %q", s)
	}
	yurl.URL = url
	return err
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
