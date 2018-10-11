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

// Backend defines backend
type Backend struct {
	Endpoint    types.YAMLUrl     `yaml:"Endpoint"`
	Type        string            `yaml:"Type"`
	Maintenance bool              `yaml:"Maintenance"`
	Region      string            `yaml:"Region"`
	Properties  map[string]string `yaml:"Properties"`
}

// BackendsMap is map of Backend
type BackendsMap map[string]Backend

// Cluster defines cluster configuration
type Cluster struct {
	// Backends should contain s3 backend urls
	Storages Storages
}

// ClustersMap is map of Cluster
type ClustersMap map[string]Cluster

type Storages []Storage

type Storage struct {
	Name                       string           `yaml:"Name"`
	BreakerProbeSize           int              `yaml:"BreakerProbeSize"`
	BreakerErrorRate           float64          `yaml:"BreakerErrorRate"`
	BreakerTimeLimit           metrics.Interval `yaml:"BreakerTimeLimit"`
	BreakerTimeLimitPercentile float64          `yaml:"BreakerTimeLimitPercentile"`
	BreakerBasicCutOutDuration metrics.Interval `yaml:"BreakerBasicCutOutDuration"`
	BreakerMaxCutOutDuration   metrics.Interval `yaml:"BreakerMaxCutOutDuration"`
	Priority                   int              `yaml:"Priority"`
	MeterResolution            metrics.Interval `yaml:"MeterResolution"`
	MeterRetention             metrics.Interval `yaml:"MeterRetention"`
}
