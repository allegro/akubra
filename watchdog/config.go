package watchdog

type Method uint

type watchdogType = string
type watchdogProps = map[string]string
type Config = map[watchdogType]watchdogProps

type ConsistencyWatchdogFactory interface {
	CreateWatchdogInstance(config *Config) (ConsistencyWatchdog, error)
}
