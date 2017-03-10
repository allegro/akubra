package metrics

//All credits to https://github.com/eBay/fabio/tree/master/metrics
import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/allegro/akubra/log"
	graphite "github.com/cyberdelia/go-metrics-graphite"
	metrics "github.com/rcrowley/go-metrics"
	"github.com/rcrowley/go-metrics/exp"
)

var pfx string

// Clear removes all metrics in Registry
func Clear() {
	log.Print("Unregistering all metrics.")
	metrics.DefaultRegistry.UnregisterAll()
}

// Mark creates and increments Meter
func Mark(name string) {
	meter := metrics.GetOrRegisterMeter(name, metrics.DefaultRegistry)
	meter.Mark(1)
}

// UpdateSince creates and update Timer
func UpdateSince(name string, since time.Time) {
	timer := metrics.GetOrRegisterTimer(name, metrics.DefaultRegistry)
	timer.UpdateSince(since)
}

// Time creates and update Timer
func Time(name string, function func()) {
	timer := metrics.GetOrRegisterTimer(name, metrics.DefaultRegistry)
	timer.Time(function)
}

// UpdateGauge changes Gauge value
func UpdateGauge(name string, value int64) {
	gauge := metrics.GetOrRegisterGauge(name, metrics.DefaultRegistry)
	gauge.Update(value)
}

func setupPrefix(cfg Config) (string, error) {
	pfx = cfg.Prefix
	if pfx == "default" {
		prefix, err := defaultPrefix()
		if err != nil {
			return "", err
		}
		pfx = prefix
	}

	if cfg.AppendDefaults {
		prefix, err := appendDefaults(cfg.Prefix)
		if err != nil {
			return "", err
		}
		pfx = prefix
	}
	return pfx, nil
}

//Init setups metrics publication
func Init(cfg Config) (err error) {
	prefix, perr := setupPrefix(cfg)
	pfx = prefix
	if perr != nil {
		return perr
	}
	err = collectSystemMetrics(cfg.Debug)
	if err != nil {
		return err
	}
	err = collectRuntimeMetrics()
	if err != nil {
		return err
	}

	switch cfg.Target {
	case "stdout":
		log.Print("Sending metrics to stdout")
		return initStdout(cfg.Interval.Duration)
	case "graphite":
		if cfg.Addr == "" {
			return errors.New("metrics: graphite addr missing")
		}
		log.Printf("Sending metrics to Graphite on %s as %q", cfg.Addr, pfx)
		return initGraphite(cfg.Addr, cfg.Interval.Duration)
	case "expvar":
		handler := exp.ExpHandler(metrics.DefaultRegistry)
		go startExpvar(cfg, handler)
		return nil

	case "":
		log.Printf("Metrics disabled")
		return nil
	default:
		return fmt.Errorf("Invalid metrics target %s", cfg.Target)
	}
}

func startExpvar(cfg Config, handler http.Handler) {
	err := http.ListenAndServe(cfg.ExpAddr, handler)
	if err != nil {
		log.Printf("Could not start exp server: %q", err.Error())
	}
}

// Clean replaces metrics path compound special chars with underscore
func Clean(s string) string {
	if s == "" {
		return "_"
	}
	s = strings.Replace(s, ".", "_", -1)
	s = strings.Replace(s, ":", "_", -1)
	return strings.ToLower(s)
}

func appendDefaults(prefix string) (string, error) {
	defaults, err := defaultPrefix()
	if err != nil {
		log.Printf("Problem with detecting defaults: %q", err.Error())
		return "", err
	}
	return prefix + "." + defaults, nil
}

// stubbed out for testing
var hostname = func() (string, error) {
	out, err := exec.Command("hostname", "-f").Output()
	if err != nil {
		log.Fatal(err)
	}
	return strings.Trim(fmt.Sprintf("%s", out), "\n "), err
}

func defaultPrefix() (string, error) {
	host, err := hostname()
	if err != nil {
		log.Printf("Problem with detecting prefix: %q", err.Error())
		return "", err
	}
	exe := filepath.Base(os.Args[0])
	return Clean(host) + "." + Clean(exe), nil
}

func initStdout(interval time.Duration) error {
	go metrics.LogScaled(metrics.DefaultRegistry, interval, time.Second, log.DefaultLogger)
	return nil
}

func initGraphite(addr string, interval time.Duration) error {
	a, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return fmt.Errorf("metrics: cannot connect to Graphite: %s", err)
	}

	go graphite.WithConfig(graphite.Config{
		DurationUnit:  time.Second,
		Registry:      metrics.DefaultRegistry,
		FlushInterval: interval,
		Prefix:        pfx,
		Addr:          a})
	return nil
}
