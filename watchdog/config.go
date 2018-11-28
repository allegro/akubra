package watchdog

type Method string

type watchdogType = string
type watchdogProps = map[string]string
type Configs = map[watchdogType]watchdogProps

type Config struct {
	Type  string        `yaml:"Type"`
	Props watchdogProps `yaml:"Props"`
}
type ConsistencyWatchdogFactory interface {
	CreateWatchdogInstance(config *Config) (ConsistencyWatchdog, error)
}
