package config

import (
	"github.com/allegro/akubra/types"
)

type typ string

const (
	// S3 represents s3 compiliant storages
	S3 typ = "S3"
	// GCS represents Google Cloud Storage compiliant storages
	GCS typ = "GCS"
	// PassThrough does not re-sign requests
	PassThrough typ = "passthrough"
)

// Backend defines backend
type Backend struct {
	Endpoint    types.YAMLUrl `yaml:"Endpoint"`
	Type        typ           `yaml:"Type"`
	Maintenance bool          `yaml:"Maintenance"`
}

// BackendsMap is map of Backend
type BackendsMap map[string]Backend

// Cluster defines cluster configuration
type Cluster struct {
	// Backends should contain s3 backend urls
	Backends []string `yaml:"Backends"`
}

// ClustersMap is map of Cluster
type ClustersMap map[string]Cluster
