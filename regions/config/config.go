package config

type ConsistencyLevel string

const (
	None = "None"
	Weak = "Weak"
	Strong = "Strong"
)

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
	// ConsistencyLevel determines how hard akubra will try to  make sure that the object is replicated on all storages
	ConsistencyLevel ConsistencyLevel `yaml:"ConsistencyLevel"`
	// ReadRepair tells akubra that it should emit sync entries when it detects inconsistencies between storage when reading data
	ReadRepair bool `yaml:"ReadRepair"`
}

// ShardingPolicies maps name with Region definition
type ShardingPolicies map[string]Policies
