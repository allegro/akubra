package balancing

// import (
// 	"testing"
// 	"time"

// 	"github.com/stretchr/testify/assert"
// )

// func TestCallStatsCollector(t *testing.T) {
// 	collector := &CallStatsCollector{}
// 	collector.Update(time.Second, true)

// 	assert.Equal(t,
// 		time.Second,
// 		collector.MeanTime(), "Mean time 2")

// 	assert.Equal(t,
// 		float64(0),
// 		collector.ErrorRate(),
// 		"Error rate should be 0")

// 	collector.Update(3*time.Second, false)
// 	assert.Equal(t,
// 		2*time.Second,
// 		collector.MeanTime(),
// 		"Mean time should be 2")
// 	assert.Equal(t,
// 		0.5,
// 		collector.ErrorRate(),
// 		"Error rate should be 0.5")
// }

// func TestCallStatsCollectorWithCapacity(t *testing.T) {
// 	size := 10
// 	collector := NewStatsCollector(size)
// 	for i := 0; i < size; i++ {
// 		collector.Update(time.Second, true)
// 	}
// 	assert.Equal(t,
// 		time.Second,
// 		collector.MeanTime(),
// 		"Mean time should be 1s")
// 	assert.Equal(t,
// 		float64(0),
// 		collector.ErrorRate(),
// 		"Error rate should be 0")

// 	for i := 0; i < size; i++ {
// 		collector.Update(2*time.Second, false)
// 	}
// 	assert.Equal(t,
// 		2*time.Second,
// 		collector.MeanTime(),
// 		"Mean time should be 1s")
// 	assert.Equal(t,
// 		float64(1),
// 		collector.ErrorRate(),
// 		"Error rate should be 1")

// }
