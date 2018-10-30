package config

// RegionCluster defines region cluster
type RegionCluster struct {
	Name   string  `yaml:"Cluster,omitempty"`
	Weight float64 `yaml:"Weight"`
}

// Region region configuration
type Region struct {
	// Multi cluster config
	Clusters []RegionCluster `yaml:"Clusters"`
	// Domains used for region matching
	Domains []string `yaml:"Domains"`
	// Default region will be applied if Host header would not match any other region
	Default bool `yaml:"Default,omitempty"`
}

// Regions maps name with Region definition
type Regions map[string]Region
