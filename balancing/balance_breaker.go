package balancing

import (
	"fmt"
	"math"
	"sort"
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
	Record(duration time.Duration, success bool) bool
	ShouldOpen() bool
}

func newBreaker(retention int, timeLimit time.Duration,
	timeLimitPercentile, errorRate float64,
	closeDelay, maxDelay time.Duration) Breaker {

	return &NodeBreaker{
		timeData:            newLenLimitCounter(retention),
		successData:         newLenLimitCounter(retention),
		rate:                errorRate,
		limit:               timeLimit,
		timeLimitPercentile: timeLimitPercentile,
		now:                 time.Now,
		closeDelay:          closeDelay,
		maxDelay:            maxDelay,
	}
}

type NodeBreaker struct {
	rate                float64
	limit               time.Duration
	timeData            *lenLimitCounter
	timeLimitPercentile float64
	successData         *lenLimitCounter
	now                 func() time.Time
	closeDelay          time.Duration
	maxDelay            time.Duration
	isOpen              bool
	openTime            time.Time
	closeTime           time.Time
	state               *openStateTracker
}

func (breaker *NodeBreaker) Record(duration time.Duration, success bool) bool {
	breaker.timeData.Add(float64(duration))
	successValue := float64(1)
	if success {
		successValue = float64(0)
	}
	breaker.successData.Add(successValue)
	return breaker.ShouldOpen()
}

func (breaker *NodeBreaker) ShouldOpen() bool {
	exceeded := breaker.limitsExceeded()
	if breaker.state != nil {
		return breaker.checkStateHalfOpen(exceeded)
	}

	if exceeded {
		breaker.openBreaker()
	}
	return exceeded
}

func (breaker *NodeBreaker) checkStateHalfOpen(exceeded bool) bool {
	state, changed := breaker.state.currentState(breaker.now(), exceeded)
	if state == closed {
		if changed {
			breaker.state = nil
		}
		return false
	}
	if state == halfopen {
		if changed {
			breaker.reset()
		}
		return false
	}
	return true
}

func (breaker *NodeBreaker) limitsExceeded() bool {
	if breaker.errorRate() > breaker.rate {
		breaker.openBreaker()
		return true
	}

	if breaker.timeData.Percentile(breaker.timeLimitPercentile) > float64(breaker.limit) {
		breaker.openBreaker()
		return true
	}
	return false
}

func (breaker *NodeBreaker) openBreaker() {
	if breaker.state != nil {
		return
	}
	breaker.state = newOpenStateTracker(
		breaker.now(), breaker.closeDelay, breaker.maxDelay)
}

func (breaker *NodeBreaker) reset() {
	breaker.timeData.Reset()
	breaker.successData.Reset()
}

func (breaker *NodeBreaker) errorRate() float64 {
	sum := breaker.successData.Sum()
	count := float64(len(breaker.successData.values))
	return sum / count
}

func newLenLimitCounter(retention int) *lenLimitCounter {
	return &lenLimitCounter{
		values: make([]float64, retention, retention),
	}
}

type lenLimitCounter struct {
	values  []float64
	nextIdx int
	mx      sync.Mutex
}

func (counter *lenLimitCounter) Add(value float64) {
	index := counter.nextIdx
	counter.values[index] = value
	counter.nextIdx = (counter.nextIdx + 1) % cap(counter.values)
}

func (counter *lenLimitCounter) Sum() float64 {
	sum := float64(0)
	for _, v := range counter.values {
		sum += v
	}
	return sum
}

func (counter *lenLimitCounter) Percentile(percentile float64) float64 {
	snapshot := make([]float64, len(counter.values))
	copy(snapshot, counter.values)
	sort.Float64s(snapshot)
	pertcentileIndex := int(math.Floor(float64(len(snapshot)) * percentile))
	return snapshot[pertcentileIndex]
}

func (counter *lenLimitCounter) Reset() {
	for idx := range counter.values {
		counter.values[idx] = 0
	}
}

type breakerState int

const (
	open     breakerState = 0
	halfopen              = iota
	closed                = iota
)

func newOpenStateTracker(start time.Time, changeDelay, maxDelay time.Duration) *openStateTracker {
	return &openStateTracker{
		lastChange:  start,
		state:       open,
		changeDelay: changeDelay,
		maxDelay:    maxDelay,
	}
}

type openStateTracker struct {
	state          breakerState
	lastChange     time.Time
	changeDelay    time.Duration
	maxDelay       time.Duration
	closeIteration float64
}

func (tracker *openStateTracker) currentDelay() time.Duration {
	durationFloat64 := float64(tracker.changeDelay) * math.Pow(2, tracker.closeIteration)

	if durationFloat64 < float64(tracker.maxDelay) {
		return time.Duration(durationFloat64)
	}
	return tracker.maxDelay
}

func (tracker *openStateTracker) currentState(now time.Time, limitsExceeded bool) (breakerState, bool) {

	if limitsExceeded && tracker.state != open {
		tracker.state = open
		tracker.lastChange = now
		tracker.closeIteration++
		return tracker.state, true
	}

	changed := false
	if now.Sub(tracker.lastChange) < tracker.currentDelay() {
		return tracker.state, changed
	}

	changed = true
	tracker.lastChange = now
	if tracker.state == open {
		tracker.state = halfopen
		return halfopen, changed
	}

	if tracker.state == halfopen {
		if limitsExceeded {
			tracker.state = open
			tracker.closeIteration++
		} else {
			tracker.state = closed
		}
	}
	return tracker.state, changed
}
