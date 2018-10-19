package balancing

import (
	"fmt"
	"math"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/allegro/akubra/log"
	"github.com/allegro/akubra/metrics"
	"github.com/allegro/akubra/storages/backend"
	"github.com/allegro/akubra/storages/config"
)

// ResponseTimeBalancer proxies calls to balancing nodes
type ResponseTimeBalancer struct {
	Nodes []Node
}

// Elect elects node and calls it with args
func (balancer *ResponseTimeBalancer) Elect(skipNodes ...Node) (Node, error) {
	start := time.Now()
	var elected Node
	for _, node := range balancer.Nodes {
		if !node.IsActive() || inSkipNodes(skipNodes, node) {
			continue
		}

		if elected == nil {
			elected = node
			continue
		}

		if nodeWeight(node) < nodeWeight(elected) {
			elected = node
		}
	}
	if elected == nil {
		return nil, ErrNoActiveNodes
	}
	// Disrupt node stats. If all nodes has zero weight only first would
	// get all the load unless response will come
	elected.UpdateTimeSpent(time.Since(start))
	return elected, nil
}

func inSkipNodes(skipNodes []Node, node Node) bool {
	for _, skipNode := range skipNodes {
		if node == skipNode {
			return true
		}
	}
	return false
}

// Node is interface of call node
type Node interface {
	Calls() float64
	TimeSpent() float64
	IsActive() bool
	SetActive(bool)
	UpdateTimeSpent(time.Duration)
}

func nodeWeight(node Node) float64 {
	return node.Calls() * node.TimeSpent()
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
		isActive:   true,
	}
}

// CallMeter implements Node interface
type CallMeter struct {
	retention  time.Duration
	resolution time.Duration
	calls      int
	now        func() time.Time
	histogram  *histogram
	isActive   bool
}

// UpdateTimeSpent aggregates data about call duration
func (meter *CallMeter) UpdateTimeSpent(duration time.Duration) {
	series := meter.histogram.pickSeries(meter.now())
	if series == nil {
		return
	}
	series.Add(float64(duration), meter.now())
}

// Calls returns number of calls in last bucket
func (meter *CallMeter) Calls() float64 {
	return meter.CallsInLastPeriod(meter.resolution)
}

// CallsInLastPeriod returns number of calls in last duration
func (meter *CallMeter) CallsInLastPeriod(period time.Duration) float64 {
	lastPeriodSeries := meter.histogram.pickLastSeries(period)
	sum := float64(0)
	now := meter.now()
	for _, series := range lastPeriodSeries {
		values := series.ValueRange(now.Add(-period), now)
		sum += float64(len(values))
	}
	return sum
}

// IsActive aseses if node should be active
func (meter *CallMeter) IsActive() bool {

	return meter.isActive
}

// SetActive sets meter state
func (meter *CallMeter) SetActive(active bool) {
	meter.isActive = active
}

// TimeSpent returns float64 repesentation of time spent in execution
func (meter *CallMeter) TimeSpent() float64 {
	allSeries := meter.histogram.pickLastSeries(meter.resolution)
	sum := float64(0)
	now := meter.now()
	for _, series := range allSeries {
		series.ValueRangeFun(now.Add(-meter.resolution), now, func(value timeValue) {
			sum += value.value
		})
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

func (series *dataSeries) ValueRangeFun(timeStart, timeEnd time.Time, fun func(timeValue)) []float64 {
	dataRange := []float64{}
	for _, timeVal := range series.data {
		if (timeStart == timeVal.date || timeStart.Before(timeVal.date)) && timeEnd.After(timeVal.date) {
			fun(timeVal)
		}
	}
	return dataRange
}

func (series *dataSeries) ValueRange(timeStart, timeEnd time.Time) []float64 {
	dataRange := []float64{}
	series.ValueRangeFun(timeStart, timeEnd, func(value timeValue) {
		dataRange = append(dataRange, value.value)
	})
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

func (h *histogram) pickSeries(at time.Time) *dataSeries {
	h.mx.Lock()
	defer h.mx.Unlock()
	idx := h.index(at)
	if idx < 0 {
		return nil
	}
	cellsNum := h.cellsCount()
	if idx >= cellsNum || idx >= len(h.data) {
		h.unshiftData(at)
		idx = h.index(at)
	}
	return h.data[idx]
}

func (h *histogram) pickLastSeries(period time.Duration) []*dataSeries {
	if period > h.retention {
		period = h.retention
	}
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

// Breaker is interface of citcuit breaker
type Breaker interface {
	Record(duration time.Duration, success bool) bool
	ShouldOpen() bool
}

func newBreaker(retention int, callTimeLimit time.Duration,
	timeLimitPercentile, errorRate float64,
	closeDelay, maxDelay time.Duration) Breaker {

	return &NodeBreaker{
		timeData:            newLenLimitCounter(retention),
		successData:         newLenLimitCounter(retention),
		rate:                errorRate,
		callTimeLimit:       callTimeLimit,
		timeLimitPercentile: timeLimitPercentile,
		now:                 time.Now,
		closeDelay:          closeDelay,
		maxDelay:            maxDelay,
	}
}

// NodeBreaker is implementation of Breaker interface
type NodeBreaker struct {
	rate                float64
	callTimeLimit       time.Duration
	timeLimitPercentile float64
	timeData            *lengthDelimitedCounter
	successData         *lengthDelimitedCounter
	now                 func() time.Time
	closeDelay          time.Duration
	maxDelay            time.Duration
	state               *openStateTracker
}

// Record collects call data and returns bool if breaker should be open
func (breaker *NodeBreaker) Record(duration time.Duration, success bool) bool {
	breaker.timeData.Add(float64(duration))
	successValue := float64(1)
	if success {
		successValue = float64(0)
	}
	breaker.successData.Add(successValue)
	return breaker.ShouldOpen()
}

// ShouldOpen checks if breaker should be open
func (breaker *NodeBreaker) ShouldOpen() bool {
	exceeded := breaker.limitsExceeded()
	if breaker.state != nil {
		return breaker.isHalfOpen(exceeded)
	}

	if exceeded {
		breaker.openBreaker()
	}
	return exceeded
}

func (breaker *NodeBreaker) isHalfOpen(exceeded bool) bool {
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

	if breaker.timeData.Percentile(breaker.timeLimitPercentile) > float64(breaker.callTimeLimit) {
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

func newLenLimitCounter(retention int) *lengthDelimitedCounter {
	return &lengthDelimitedCounter{
		values: make([]float64, retention, retention),
	}
}

type lengthDelimitedCounter struct {
	values  []float64
	nextIdx int
	mx      sync.Mutex
}

// Add acumates new values
func (counter *lengthDelimitedCounter) Add(value float64) {
	counter.mx.Lock()
	defer counter.mx.Unlock()
	index := counter.nextIdx
	counter.values[index] = value
	counter.nextIdx = (counter.nextIdx + 1) % cap(counter.values)
}

// Sum returns sum of values
func (counter *lengthDelimitedCounter) Sum() float64 {
	sum := float64(0)
	for _, v := range counter.values {
		sum += v
	}
	return sum
}

// Percentile return value for given percentile
func (counter *lengthDelimitedCounter) Percentile(percentile float64) float64 {
	snapshot := make([]float64, len(counter.values))
	copy(snapshot, counter.values)
	sort.Float64s(snapshot)
	pertcentileIndex := int(math.Floor(float64(len(snapshot)) * percentile))
	return snapshot[pertcentileIndex]
}

func (counter *lengthDelimitedCounter) Reset() {
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
	multiplier := int(math.Pow(2, tracker.closeIteration))
	delayDuration := tracker.changeDelay * time.Duration(multiplier)

	if delayDuration < tracker.maxDelay {
		return delayDuration
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

// MeasuredStorage coordinates metrics collection
type MeasuredStorage struct {
	Node
	Breaker
	http.RoundTripper
	Name           string
	watcherStarted bool
}

// RoundTrip implements http.RoundTripper
func (ms *MeasuredStorage) RoundTrip(req *http.Request) (*http.Response, error) {
	start := time.Now()
	reqID, _ := req.Context().Value(log.ContextreqIDKey).(string)
	log.Debug("MeasuredStorage: Got request id %s", reqID)
	resp, err := ms.RoundTripper.RoundTrip(req)
	duration := time.Since(start)
	success := backend.IsSuccessful(resp, err)
	open := ms.Breaker.Record(duration, success)
	ms.Node.UpdateTimeSpent(duration)
	ms.Node.SetActive(!open)
	raportMetrics(ms.RoundTripper, start, open)
	return resp, err
}

func (ms *MeasuredStorage) watchBreakerStatus(interval time.Duration) {
	if ms.watcherStarted {
		return
	}
	go func() {
		for {
			<-time.After(interval)
			ms.Node.SetActive(!ms.Breaker.ShouldOpen())
		}
	}()
	ms.watcherStarted = true
}

func raportMetrics(rt http.RoundTripper, since time.Time, open bool) {
	if b, ok := rt.(*backend.Backend); ok {
		prefix := fmt.Sprintf("reqs.backend.%s.balancer", b.Name)
		metrics.UpdateSince(prefix+".duration", since)
		if open {
			metrics.UpdateGauge(prefix+".open", 1)
		} else {
			metrics.UpdateGauge(prefix+".open", 0)
		}
	}
}

// NewBalancerPrioritySet configures prioritized balancers stack
func NewBalancerPrioritySet(storagesConfig config.Storages, backends map[string]http.RoundTripper) *BalancerPrioritySet {
	priorities := make([]int, 0)
	priotitiesFilter := make(map[int]struct{})
	priorityStorage := make(map[int][]*MeasuredStorage)
	for _, storageConfig := range storagesConfig {
		breaker := newBreaker(storageConfig.BreakerProbeSize,
			storageConfig.BreakerCallTimeLimit.Duration,
			storageConfig.BreakerCallTimeLimitPercentile,
			storageConfig.BreakerErrorRate,
			storageConfig.BreakerBasicCutOutDuration.Duration,
			storageConfig.BreakerMaxCutOutDuration.Duration,
		)
		meter := newCallMeter(storageConfig.MeterRetention.Duration, storageConfig.MeterResolution.Duration)
		backend, ok := backends[storageConfig.Name]
		if !ok {
			log.Fatalf("No defined storage %s\n", storageConfig.Name)
		}
		if _, ok := priotitiesFilter[storageConfig.Priority]; !ok {
			priorities = append(priorities, storageConfig.Priority)
			priotitiesFilter[storageConfig.Priority] = struct{}{}
		}

		mstorage := &MeasuredStorage{Breaker: breaker, Node: Node(meter), RoundTripper: backend, Name: storageConfig.Name}
		if _, ok := priorityStorage[storageConfig.Priority]; !ok {
			priorityStorage[storageConfig.Priority] = make([]*MeasuredStorage, 0, 1)
		}
		mstorage.watchBreakerStatus(storageConfig.BreakerBasicCutOutDuration.Duration)
		priorityStorage[storageConfig.Priority] = append(
			priorityStorage[storageConfig.Priority], mstorage)
	}
	sort.Ints(priorities)
	bps := &BalancerPrioritySet{balancers: []*ResponseTimeBalancer{}}
	for _, key := range priorities {
		nodes := make([]Node, 0)
		for _, node := range priorityStorage[key] {
			nodes = append(nodes, Node(node))
		}
		balancer := &ResponseTimeBalancer{Nodes: nodes}
		bps.balancers = append(bps.balancers, balancer)
	}
	return bps
}

// BalancerPrioritySet selects storage by priority and availability
type BalancerPrioritySet struct {
	balancers []*ResponseTimeBalancer
}

// GetMostAvailable returns balancer member
func (bps *BalancerPrioritySet) GetMostAvailable(skipNodes ...Node) *MeasuredStorage {
	for level, balancer := range bps.balancers {
		node, err := balancer.Elect(skipNodes...)
		if err == ErrNoActiveNodes {
			log.Println("Changed prioryty level to %s", level)
			continue
		}
		return node.(*MeasuredStorage)
	}
	return nil
}
