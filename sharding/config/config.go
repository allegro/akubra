package config

import (
	"fmt"
	"net/url"
	"strings"

	"errors"

	units "github.com/docker/go-units"
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

// AdditionalHeaders type fields in yaml configuration will parse list of special headers
type AdditionalHeaders map[string]string

// HumanSizeUnits type for max. payload body size in bytes
type HumanSizeUnits struct {
	SizeInBytes int64
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

// UnmarshalYAML for AdditionalHeaders
func (ah *AdditionalHeaders) UnmarshalYAML(unmarshal func(interface{}) error) error {
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
	*ah = AdditionalHeaders(headers)
	return nil
}

// UnmarshalYAML for HumanSizeUnits
func (hsu *HumanSizeUnits) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var size string
	if err := unmarshal(&size); err != nil {
		return err
	}
	value, err := units.FromHumanSize(size)
	if err != nil {
		return fmt.Errorf("Unable to parse BodyMaxSize: %s" + err.Error())
	}
	if value < 1 {
		return errors.New("BodyMaxSize must be greater than zero")
	}
	hsu.SizeInBytes = value
	return nil
}
