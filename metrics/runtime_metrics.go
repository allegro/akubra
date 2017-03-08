package metrics

import (
	"runtime"

	metrics "github.com/rcrowley/go-metrics"
)

const goroutinesNumGauge = "runtime.goroutines_num"

func collectRuntimeMetrics() error {
	return metrics.Register(goroutinesNumGauge, runtimeGauge{value: func() int64 { return int64(runtime.NumGoroutine()) }})
}

type runtimeGauge struct {
	value func() int64
}

func (g runtimeGauge) Value() int64 {
	return g.value()
}

func (g runtimeGauge) Snapshot() metrics.Gauge { return metrics.GaugeSnapshot(g.Value()) }

func (runtimeGauge) Update(int64) {}
