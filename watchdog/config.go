package watchdog

// Method is the ConsistencyRecord type
type Method string

type watchdogType = string
type watchdogProps = map[string]string

// Config is watchdog type
type Config struct {
	ObjectVersionHeaderName string        `yaml:"ObjectVersionHeaderName"`
	Type                    string        `yaml:"Type"`
	Props                   watchdogProps `yaml:"Props"`
}

// ConsistencyWatchdogFactory creates ConsistencyWatchdogs
type ConsistencyWatchdogFactory interface {
	CreateWatchdogInstance(config *Config) (ConsistencyWatchdog, error)
}
