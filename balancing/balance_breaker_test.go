package balancing

import (
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/allegro/akubra/metrics"
	"github.com/allegro/akubra/storages/config"
	"github.com/stretchr/testify/require"
)

func TestResponseTimeBalancingMemberElects(t *testing.T) {
	balancer := &ResponseTimeBalancer{}
	member, err := balancer.Elect()
	require.Error(t, err)
	require.Nil(t, member)

	balancer = &ResponseTimeBalancer{
		Nodes: []Node{
			&nodeMock{active: true},
		},
	}

	member, err = balancer.Elect()
	require.NoError(t, err)
	require.NotNil(t, member)

	firstNode := &nodeMock{err: fmt.Errorf("first"), time: 1, calls: 1, active: true}
	secondNode := &nodeMock{err: fmt.Errorf("second"), time: 1, calls: 2, active: true}
	balancer = &ResponseTimeBalancer{
		Nodes: []Node{
			firstNode,
			secondNode,
		},
	}
	member, err = balancer.Elect()
	require.NoError(t, err)
	require.Equal(t, firstNode, member)

	balancer = &ResponseTimeBalancer{
		Nodes: []Node{
			&nodeMock{err: fmt.Errorf("first"), time: 1, calls: 1, active: false},
			&nodeMock{err: fmt.Errorf("second"), time: 1, calls: 2, active: true},
		},
	}

	member, err = balancer.Elect()
	require.NoError(t, err)
	require.Equal(t, secondNode, member)

	balancer = &ResponseTimeBalancer{
		Nodes: []Node{
			&nodeMock{err: fmt.Errorf("first"), time: 1, calls: 1, active: false},
			&nodeMock{err: fmt.Errorf("second"), time: 1, calls: 2, active: false},
		},
	}
	member, err = balancer.Elect()
	require.Error(t, err)
	require.Equal(t, nil, member)
}

type nodeMock struct {
	err    error
	calls  float64
	time   float64
	active bool
	member interface{}
}

func (node *nodeMock) Member() interface{} {
	return node.member
}

func (node *nodeMock) Calls() float64 {
	return node.calls
}

func (node *nodeMock) Time() float64 {
	return node.time
}

func (node *nodeMock) IsActive() bool {
	return node.active
}

func (node *nodeMock) SetActive(bool) {
}

func (node *nodeMock) Update(time.Duration) {
}

func TestCallMeter(t *testing.T) {
	callMeter := newCallMeter(5*time.Second, 5*time.Second)
	require.Implements(t, (*Node)(nil), callMeter)

	callMeter.Update(time.Millisecond)
	require.Equal(t, float64(time.Millisecond), callMeter.Time(), "Time summary missmatch")
	require.Equal(t, float64(1), callMeter.Calls(), "Number of calls missmatch")
}

func TestCallMeterConcurrency(t *testing.T) {
	numberOfSamples := 10000
	sampleDuration := time.Millisecond
	waitGroup := &sync.WaitGroup{}
	waitGroup.Add(numberOfSamples)
	callMeter := newCallMeter(5*time.Second, 5*time.Second)
	for i := 0; i < numberOfSamples; i++ {
		go func() {
			callMeter.Update(sampleDuration)
			waitGroup.Done()
		}()
	}
	waitGroup.Wait()
	require.Equal(t, float64(numberOfSamples*int(sampleDuration)), callMeter.Time())
	require.Equal(t, float64(numberOfSamples), callMeter.Calls())
}

func TestCallMeterRetention(t *testing.T) {
	numberOfSamples := 100
	timer := &mockTimer{
		baseTime:   time.Now(),
		advanceDur: 100 * time.Millisecond}

	callMeter := newCallMeter(5*time.Second, 1*time.Second)
	callMeter.now = timer.now

	for i := 0; i < numberOfSamples; i++ {
		callMeter.Update(time.Millisecond)
		timer.advance()
	}

	require.InDelta(t, float64(callMeter.resolution/timer.advanceDur), callMeter.Calls(), float64(1))
	period := 2 * time.Second
	require.InDelta(t, float64(period/timer.advanceDur), callMeter.CallsIn(period), float64(1))
	timer.advanceDur = 2 * time.Second
	timer.advance()
	require.Equal(t, float64(0), callMeter.Calls())
}

type mockTimer struct {
	baseTime   time.Time
	advanceDur time.Duration
	mx         sync.Mutex
}

func (timer *mockTimer) now() time.Time {
	timer.mx.Lock()
	defer timer.mx.Unlock()
	return timer.baseTime
}

func (timer *mockTimer) advance() {
	timer.mx.Lock()
	defer timer.mx.Unlock()
	timer.baseTime = timer.baseTime.Add(timer.advanceDur)
}

func TestHistogramRetention(t *testing.T) {
	retention := 5 * time.Second
	resolution := 1 * time.Second
	hist := newTimeHistogram(retention, resolution)
	series := hist.pickSeries(time.Now())
	require.NotNil(t, series)
}

func TestBreaker(t *testing.T) {
	breaker := makeTestBreaker()
	require.Implements(t, (*Breaker)(nil), breaker)

	breaker.Record(100*time.Millisecond, true)
	require.False(t, breaker.ShouldOpen())

	breaker = makeTestBreaker()
	for i := 0; i < 100; i++ {
		breaker.Record(1100*time.Millisecond, true)
	}
	require.True(t, breaker.ShouldOpen())

	breaker = makeTestBreaker()
	breaker.Record(1*time.Millisecond, false)
	require.False(t, breaker.ShouldOpen())

	breaker = makeTestBreaker()
	for i := 0; i < 11; i++ {
		breaker.Record(1*time.Millisecond, false)
	}
	require.True(t, breaker.ShouldOpen())
}

func makeTestBreaker() Breaker {
	errorRate := 0.1
	timeLimit := time.Second
	retention := 100
	timeLimitPercentile := 0.9
	closeDelay := time.Second
	maxDelay := 4 * time.Second
	breaker := newBreaker(
		retention, timeLimit, timeLimitPercentile,
		errorRate, closeDelay, maxDelay)

	return breaker
}

func makeTestBreakerWithTimer(now func() time.Time) Breaker {
	breaker := makeTestBreaker()
	nodebreaker := breaker.(*NodeBreaker)
	nodebreaker.now = now
	return nodebreaker
}

func TestBreakerRecoveryPeriodsProgression(t *testing.T) {
	timer := &mockTimer{
		baseTime:   time.Now(),
		advanceDur: 1000 * time.Millisecond}

	breaker := makeTestBreakerWithTimer(timer.now)
	openBreaker(breaker)
	opentime := timer.now()
	checkOpenFor(t, time.Second, breaker, timer)
	require.False(t, breaker.ShouldOpen(),
		fmt.Sprintf("should be in halfclosed state after %s", timer.now().Sub(opentime)))

	openBreaker(breaker)
	require.True(t, breaker.ShouldOpen(), fmt.Sprintf("should be in open"))

	checkOpenFor(t, 2*time.Second, breaker, timer)
	require.False(t, breaker.ShouldOpen())

	openBreaker(breaker)
	checkOpenFor(t, 4*time.Second, breaker, timer)
	require.False(t, breaker.ShouldOpen())

	openBreaker(breaker)
	checkOpenFor(t, 4*time.Second, breaker, timer)
	require.False(t, breaker.ShouldOpen())
}

func TestBreakerRecoveryPeriodsProgressionResetIfOpen(t *testing.T) {
	timer := &mockTimer{
		baseTime:   time.Now(),
		advanceDur: 1100 * time.Millisecond}

	breaker := makeTestBreakerWithTimer(timer.now)
	openBreaker(breaker)
	checkOpenFor(t, time.Second, breaker, timer)
	require.False(t, breaker.ShouldOpen())

	timer.advance()
	require.False(t, breaker.ShouldOpen())
	openBreaker(breaker)
	checkOpenFor(t, time.Second, breaker, timer)
	require.False(t, breaker.ShouldOpen(), "breaker should be closed after stats reset")
}

// func TestConcurentBreakerCalls(t *testing.T) {
// 	timer := &mockTimer{
// 		baseTime:   time.Now(),
// 		advanceDur: 1000 * time.Millisecond}

// 	breaker := makeTestBreakerWithTimer(timer.now)
// 	asyncOpenBreaker(breaker)
// }

func openBreaker(breaker Breaker) {
	for i := 0; i < 11; i++ {
		breaker.Record(1*time.Millisecond, false)
	}
}

func checkOpenFor(t *testing.T, d time.Duration, breaker Breaker, timer *mockTimer) {
	start := timer.now()
	for timer.now().Sub(start) < d {
		require.True(t, breaker.ShouldOpen(),
			fmt.Sprintf("braker closed after %s", timer.now().Sub(start)))
		timer.advance()
	}
}

func TestPriorityLayersPicker(t *testing.T) {
	config := config.Storages{
		{
			Name:                       "first-a",
			Priority:                   0,
			BreakerProbeSize:           10,
			BreakerErrorRate:           0.09,
			BreakerTimeLimit:           metrics.Interval{500 * time.Millisecond},
			BreakerTimeLimitPercentile: 0.9,
			BreakerBasicCutOutDuration: metrics.Interval{time.Second},
			BreakerMaxCutOutDuration:   metrics.Interval{180 * time.Second},
			MeterResolution:            metrics.Interval{5 * time.Second},
			MeterRetention:             metrics.Interval{10 * time.Second},
		},
		{
			Name:                       "first-b",
			Priority:                   0,
			BreakerProbeSize:           10,
			BreakerErrorRate:           0.09,
			BreakerTimeLimit:           metrics.Interval{500 * time.Millisecond},
			BreakerTimeLimitPercentile: 0.9,
			BreakerBasicCutOutDuration: metrics.Interval{time.Second},
			BreakerMaxCutOutDuration:   metrics.Interval{180 * time.Second},
			MeterResolution:            metrics.Interval{5 * time.Second},
			MeterRetention:             metrics.Interval{10 * time.Second},
		},
		{
			Name:                       "second",
			Priority:                   1,
			BreakerProbeSize:           1000,
			BreakerErrorRate:           0.1,
			BreakerTimeLimit:           metrics.Interval{500 * time.Millisecond},
			BreakerTimeLimitPercentile: 0.9,
			BreakerBasicCutOutDuration: metrics.Interval{time.Second},
			BreakerMaxCutOutDuration:   metrics.Interval{180 * time.Second},
			MeterResolution:            metrics.Interval{5 * time.Second},
			MeterRetention:             metrics.Interval{10 * time.Second},
		},
	}
	errFirstStorageResponse := fmt.Errorf("Error from first-a")
	errSecondStorageResponse := fmt.Errorf("Error from first-b")
	errThirdStorageResponse := fmt.Errorf("Error from second-b")
	backends := map[string]http.RoundTripper{
		"first-a": &MockRoundTripper{err: errFirstStorageResponse},
		"first-b": &MockRoundTripper{err: errSecondStorageResponse},
		"second":  &MockRoundTripper{err: errThirdStorageResponse},
	}
	balancerSet := NewBalancerPrioritySet(config, backends)
	require.NotNil(t, balancerSet)
	fmt.Println("First expected")

	member := balancerSet.GetMostAvailable()
	require.NotNil(t, member, "Member should be not nil")
	require.Implements(t, (*Node)(nil), member, "Member should implement Node interface")
	require.Implements(t, (*Breaker)(nil), member, "Member should implement Breaker interface")
	require.Implements(t, (*http.RoundTripper)(nil), member, "Member should implement `http.RoundTripper` interface")

	resp, err := member.RoundTrip(&http.Request{})

	require.Equal(t, errFirstStorageResponse, err)
	require.Nil(t, resp, err)
	fmt.Println("Second expected")

	member = balancerSet.GetMostAvailable()
	resp, err = member.RoundTrip(&http.Request{})
	require.Equal(t, errSecondStorageResponse, err)
	require.Nil(t, resp, err)

	fmt.Println("Third expected")
	member = balancerSet.GetMostAvailable()
	resp, err = member.RoundTrip(&http.Request{})
	require.Equal(t, errThirdStorageResponse, err)
	require.Nil(t, resp, err)
}

type MockRoundTripper struct {
	err error
}

func (mrt *MockRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return nil, mrt.err
}

var ErrMockResponse = fmt.Errorf("Mock error")
