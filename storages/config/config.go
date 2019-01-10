package config

import (
	"github.com/allegro/akubra/metrics"
	"github.com/allegro/akubra/types"
)

const (
	// S3 represents s3 compiliant storages
	S3 string = "S3"
	// GCS represents Google Cloud Storage compiliant storages
	GCS = "GCS"
	// Passthrough does not re-sign requests
	Passthrough = "passthrough"
)

// Storage defines backend
type Storage struct {
	Backend     types.YAMLUrl     `yaml:"Backend"`
	Type        string            `yaml:"Type"`
	Maintenance bool              `yaml:"Maintenance"`
	Properties  map[string]string `yaml:"Properties"`
}

// StoragesMap is map of Backend
type StoragesMap map[string]Storage

// Shard defines shard storages configuration
type Shard struct {
	Storages Storages `yaml:"Storages"`
}

// ShardsMap is map of Cluster
type ShardsMap map[string]Shard

// Storages is lists of storages
type Storages []StorageBreakerProperties

// StorageBreakerProperties describes storage usage requirements
type StorageBreakerProperties struct {
	Name                           string           `yaml:"Name"`
	BreakerProbeSize               int              `yaml:"BreakerProbeSize"`
	BreakerErrorRate               float64          `yaml:"BreakerErrorRate"`
	BreakerCallTimeLimit           metrics.Interval `yaml:"BreakerCallTimeLimit"`
	BreakerCallTimeLimitPercentile float64          `yaml:"BreakerCallTimeLimitPercentile"`
	BreakerBasicCutOutDuration     metrics.Interval `yaml:"BreakerBasicCutOutDuration"`
	BreakerMaxCutOutDuration       metrics.Interval `yaml:"BreakerMaxCutOutDuration"`
	Priority                       int              `yaml:"Priority"`
	MeterResolution                metrics.Interval `yaml:"MeterResolution"`
	MeterRetention                 metrics.Interval `yaml:"MeterRetention"`
}
