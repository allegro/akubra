package config

type watchdogProps = map[string]string

// WatchdogConfig is watchdog type
type WatchdogConfig struct {
	ObjectVersionHeaderName string        `yaml:"ObjectVersionHeaderName"`
	Type                    string        `yaml:"Type"`
	Props                   watchdogProps `yaml:"Props"`
}
