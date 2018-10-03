package balancing

import (
	"fmt"
	"math"
	"sync"
	"time"
)

// ResponseTimeBalancer proxies calls to balancing nodes
type ResponseTimeBalancer struct {
	Nodes []Node
}

// Elect elects node and call it with args
func (balancer *ResponseTimeBalancer) Elect() (Node, error) {
	var elected Node

	for _, node := range balancer.Nodes {
		if !node.IsActive() {
			continue
		}

		if elected == nil {
			elected = node
		}

		if nodeWeight(node) < nodeWeight(elected) {
			elected = node
		}
	}
	if elected == nil {
		return nil, ErrNoActiveNodes
	}
	return elected, nil
}

// Node is interface of call node
type Node interface {
	Calls() float64
	Time() float64
	IsActive() bool
	SetActive(bool)
	Update(time.Duration)
}

func nodeWeight(node Node) float64 {
	return node.Calls() * node.Time()
}

var (
	// ErrNoActiveNodes is issued if all nodes are inactive
	ErrNoActiveNodes = fmt.Errorf("Balancer has no nodes to call")
)

func newCallMeter(retention, resolution time.Duration) *CallMeter {
	return &CallMeter{
		retention:  retention,
		resolution: resolution,
		histogram:  newTimeHistogram(retention, resolution),
		now:        time.Now,
	}
}

// CallMeter implements Node interface
type CallMeter struct {
	retention  time.Duration
	resolution time.Duration
	calls      int
	now        func() time.Time
	duration   time.Duration
	histogram  *histogram
	active     bool
}

func (meter *CallMeter) pickSeries(t time.Time) {
	t = t.Round(meter.resolution)
}

// Update aggregates data about call duration
func (meter *CallMeter) Update(duration time.Duration) {
	series := meter.histogram.pickSeries(meter.now())
	if series == nil {
		return
	}
	println("added")
	series.Add(float64(duration), meter.now())
}

// Calls returns number of calls in last bucket
func (meter *CallMeter) Calls() float64 {
	return meter.CallsIn(meter.resolution)
}

// CallsIn returns number of calls in last duration
func (meter *CallMeter) CallsIn(period time.Duration) float64 {
	allSeries := meter.histogram.pickLastSeries(period)
	sum := float64(0)
	now := meter.now()
	for _, series := range allSeries {
		values := series.ValueRange(now.Add(-period), now)
		sum += float64(len(values))
	}
	return sum
}

// IsActive aseses if node should be active
func (meter *CallMeter) IsActive() bool {
	return meter.active
}

// SetActive sets meter state
func (meter *CallMeter) SetActive(active bool) {
	meter.active = active
}

// Time returns float64 repesentation of time spent on execution
func (meter *CallMeter) Time() float64 {
	allSeries := meter.histogram.pickLastSeries(meter.resolution)
	sum := float64(0)
	now := meter.now()
	for _, series := range allSeries {
		values := series.ValueRange(now.Add(-meter.resolution), now)
		for _, value := range values {
			sum += value
		}
	}

	return sum
}

type dataSeries struct {
	data []timeValue
	mx   sync.Mutex
}

func (series *dataSeries) Add(value float64, dateTime time.Time) {
	series.mx.Lock()
	defer series.mx.Unlock()
	series.data = append(series.data, timeValue{dateTime, value})
}

func (series *dataSeries) ValueRange(timeStart, timeEnd time.Time) []float64 {
	dataRange := []float64{}
	for _, timeVal := range series.data {
		if (timeStart == timeVal.date || timeStart.Before(timeVal.date)) && timeEnd.After(timeVal.date) {
			dataRange = append(dataRange, timeVal.value)
		}
	}
	return dataRange
}

type timeValue struct {
	date  time.Time
	value float64
}

func newTimeHistogram(retention, resolution time.Duration) *histogram {
	return &histogram{
		t0:         time.Now(),
		resolution: resolution,
		retention:  retention,
		now:        time.Now,
		mx:         sync.Mutex{},
	}
}

type histogram struct {
	t0         time.Time
	retention  time.Duration
	resolution time.Duration
	data       []*dataSeries
	now        func() time.Time
	mx         sync.Mutex
}

func (h *histogram) pickSeries(now time.Time) *dataSeries {
	h.mx.Lock()
	defer h.mx.Unlock()
	idx := h.index(now)
	if idx < 0 {
		return nil
	}
	cellsNum := h.cellsCount()
	if idx >= cellsNum || idx >= len(h.data) {
		h.unshiftData(now)
		idx = h.index(now)
	}
	return h.data[idx]
}

func (h *histogram) pickLastSeries(period time.Duration) []*dataSeries {
	if period > h.retention {
		period = h.retention
	}
	println(h.t0.Format(time.RFC3339Nano), h.now().Format(time.RFC3339Nano))
	h.unshiftData(h.now())
	seriesCount := int(math.Ceil(float64(period)/float64(h.resolution))) + 1
	return h.data[len(h.data)-seriesCount:]
}

func (h *histogram) index(now time.Time) int {
	sinceStart := float64(now.Sub(h.t0))
	idx := math.Floor(sinceStart / float64(h.resolution))
	fmt.Printf("Index sinceStart %f index %f\n", now.Sub(h.t0).Seconds(), idx)
	return int(idx)
}

func (h *histogram) cellsCount() int {
	return int(math.Ceil(float64(h.retention)/float64(h.resolution))) + 1
}

func (h *histogram) growSeries() {
	for len(h.data) < h.cellsCount() {
		h.data = append(h.data, &dataSeries{mx: sync.Mutex{}})
	}
}

func (h *histogram) unshiftData(now time.Time) {
	idx := h.index(now)
	cellsNum := h.cellsCount()
	shiftSize := idx - (cellsNum - 1)
	if shiftSize > 0 {
		h.t0 = h.t0.Add(time.Duration(shiftSize) * h.resolution)
		h.data = h.data[shiftSize:]
	}
	h.growSeries()
}

type Breaker interface {
	Node
	Record(time.Duration)
	Open()
	Close()
}

func newBreaker(node Node, timeLimit time.Duration) Breaker {
	return &NodeBreaker{}
}

type NodeBreaker struct {
	*CallMeter
}

func (breaker *NodeBreaker) Close() {
	breaker.CallMeter.active = true
}

func (breaker *NodeBreaker) Open() {
	breaker.CallMeter.active = false
}

func (breaker *NodeBreaker) Record(t time.Duration) {
	breaker.Update(t)
	breaker.CallMeter.active = false
}
