package watchdog

type Method uint

type Config struct {
	WatchdogType  string
	WatchdogProps map[string]string
}

type ConsistencyWatchdogFactory interface {
	CreateWatchdogInstance(config *Config) (ConsistencyWatchdog, error)
}
