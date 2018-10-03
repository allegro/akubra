package balancing

import (
	"fmt"
	"sync"
	"testing"
	"time"

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

func TestNodeBreaker(t *testing.T) {
	// errorRateLimit := 0.1
	// timeLimit := 2 * time.Second
	// node := &nodeMock{err: fmt.Errorf("first"), time: 1, calls: 1, active: false}
	// breaker := newBreaker(node, timeLimit)
}
