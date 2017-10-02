package metrics

//All credits to https://github.com/eBay/fabio/tree/master/metrics
import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	fqdn "github.com/ShowMax/go-fqdn"
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

func setupPrefix(cfg Config) string {
	pfx = cfg.Prefix
	if pfx == "default" {
		pfx = defaultPrefix()
	}

	if cfg.AppendDefaults {
		pfx = appendDefaults(cfg.Prefix)
	}
	return pfx
}

//Init setups metrics publication
func Init(cfg Config) (err error) {
	pfx = setupPrefix(cfg)

	err = collectSystemMetrics(cfg)
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
		percentiles := cfg.Percentiles
		if len(percentiles) == 0 {
			percentiles = append(percentiles, []float64{0.75, 0.95, 0.99, 0.999}...)
		}
		return initGraphite(cfg.Addr, cfg.Interval.Duration, percentiles)
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

func appendDefaults(prefix string) string {
	defaults := defaultPrefix()
	return prefix + "." + defaults
}

// stubbed out for testing
var hostname = func() string {
	return fqdn.Get()
}

func defaultPrefix() string {
	host := hostname()
	exe := filepath.Base(os.Args[0])
	return Clean(host) + "." + Clean(exe)
}

func initStdout(interval time.Duration) error {
	go metrics.LogScaled(metrics.DefaultRegistry, interval, time.Second, log.New(os.Stdout, "", log.Lshortfile))
	return nil
}

func initGraphite(addr string, interval time.Duration, percentiles []float64) error {
	a, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return fmt.Errorf("metrics: cannot connect to Graphite: %s", err)
	}

	go graphite.WithConfig(graphite.Config{
		DurationUnit:  time.Second,
		Registry:      metrics.DefaultRegistry,
		FlushInterval: interval,
		Prefix:        pfx,
		Percentiles:   percentiles,
		Addr:          a})
	return nil
}
