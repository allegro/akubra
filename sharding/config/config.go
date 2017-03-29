package config

import (
	"fmt"
	"net/url"
	"strings"
)

// ClusterConfig defines cluster configuration
type ClusterConfig struct {
	// Backends should contain s3 backend urls
	Backends []YAMLUrl `yaml:"Backends,omitempty"`
	// Type, currently replicator is only option
	Type string `yaml:"Type,omitempty"`
	// Points how much load cluster should handle
	Weight int `yaml:"Weight,omitempty"`
	// Cluster type specific options
	Options map[string]string `yaml:"Options,omitempty"`
}

// ClientConfig keeps information about client setup
type ClientConfig struct {
	// Client name
	Name string `yaml:"Name,omitempty" validate:"regexp=^([a-zA-Z0-9_-]+)$"`
	// List of clusters name
	Clusters []string `yaml:"Clusters,omitempty" validate:"NoEmptyValuesSlice=Clusters,UniqueValuesSlice=Clusters"`
}

// YAMLUrl type fields in yaml configuration will parse urls
type YAMLUrl struct {
	*url.URL
}

// SyncLogMethod type fields in yaml configuration will parse list of HTTP methods
type SyncLogMethod struct {
	Method string
}

// AdditionalHeaders type fields in yaml configuration will parse list of special headers
type AdditionalHeaders map[string]string

// UnmarshalYAML for YAMLUrl
func (j *YAMLUrl) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}
	url, err := url.Parse(s)
	if url.Host == "" {
		return fmt.Errorf("url should match proto://host[:port]/path scheme - got %q", s)
	}
	j.URL = url
	return err
}

// UnmarshalYAML for SyncLogMethod
func (j *SyncLogMethod) UnmarshalYAML(unmarshal func(interface{}) error) error {
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
	j.Method = method
	return nil
}

// UnmarshalYAML for AdditionalHeaders
func (j *AdditionalHeaders) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var headers map[string]string
	if err := unmarshal(&headers); err != nil {
		return err
	}

	for key, value := range headers {
		if strings.TrimSpace(key) == "" {
			return fmt.Errorf("Empty additional header with value: %q", value)
		}
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("Empty additional header with key: %q", key)
		}
	}
	return nil
}
