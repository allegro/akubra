package config

// Policy defines region cluster
type Policy struct {
	ShardName string  `yaml:"ShardName"`
	Weight    float64 `yaml:"Weight"`
}

// Policies region configuration
type Policies struct {
	// Multi cluster config
	Shards []Policy `yaml:"Shards"`
	// Domains used for region matching
	Domains []string `yaml:"Domains"`
	// Default region will be applied if Host header would not match any other region
	Default bool `yaml:"Default"`
}

// ShardingPolicies maps name with Region definition
type ShardingPolicies map[string]Policies
