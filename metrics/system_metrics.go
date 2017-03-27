package metrics

import (
	"runtime"

	metrics "github.com/rcrowley/go-metrics"
)

const allocGauge = "runtime.mem.bytes_allocated_and_not_yet_freed"
const sysGauge = "runtime.mem.os_mem"
const heapObjectsGauge = "runtime.mem.total_number_of_allocated_objects"
const totalPauseGauge = "runtime.mem.pause_total_ns"
const lastPauseGauge = "runtime.mem.last_pause"

func collectSystemMetrics(cfg Config) (err error) {
	if cfg.Debug {
		metrics.RegisterRuntimeMemStats(metrics.DefaultRegistry)
		go metrics.CaptureRuntimeMemStats(metrics.DefaultRegistry, cfg.Interval.Duration)
		return nil
	}
	err = metrics.Register(allocGauge, baseGauge{value: func(memStats runtime.MemStats) int64 { return int64(memStats.Alloc) }})
	if err != nil {
		return err
	}
	err = metrics.Register(sysGauge, baseGauge{value: func(memStats runtime.MemStats) int64 { return int64(memStats.Sys) }})
	if err != nil {
		return err
	}
	err = metrics.Register(heapObjectsGauge, baseGauge{value: func(memStats runtime.MemStats) int64 { return int64(memStats.HeapObjects) }})
	if err != nil {
		return err
	}
	err = metrics.Register(totalPauseGauge, baseGauge{value: func(memStats runtime.MemStats) int64 { return int64(memStats.PauseTotalNs) }})
	if err != nil {
		return err
	}
	return metrics.Register(lastPauseGauge, baseGauge{value: func(memStats runtime.MemStats) int64 { return int64(memStats.PauseNs[(memStats.NumGC+255)%256]) }})
}

type baseGauge struct {
	value func(runtime.MemStats) int64
}

func (g baseGauge) Value() int64 {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	return g.value(memStats)
}

func (g baseGauge) Snapshot() metrics.Gauge { return metrics.GaugeSnapshot(g.Value()) }

func (baseGauge) Update(int64) {}
