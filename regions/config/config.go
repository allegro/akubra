package config

type ConsistencyLevel string

const (
	//None says that the request shouldn't be logged at all
	None   ConsistencyLevel = "None"
	//Weak says that the request should be logged, but if an error occurs on logging, then the request may continue
	Weak   ConsistencyLevel = "Weak"
	//Strong says that the request must be logged and can't proceed without an entry in the log
	Strong ConsistencyLevel = "Strong"
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
