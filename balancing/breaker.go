package balancing

// import (
// 	"time"
// )

// // NewStatsCollector creates CallStatsCollector
// func NewStatsCollector(size int) *CallStatsCollector {
// 	return &CallStatsCollector{
// 		size:          size,
// 		errorRates:    dataSeries{size: size},
// 		durationStats: dataSeries{size: size}}
// }

// // CallStatsCollector collects most important call statistics
// type CallStatsCollector struct {
// 	durationStats dataSeries
// 	errorRates    dataSeries
// 	size          int
// }

// // Update updates counters
// func (collector *CallStatsCollector) Update(duration time.Duration, success bool) {
// 	var successValue float64
// 	if success {
// 		successValue = 1
// 	}
// 	collector.errorRates.update(successValue)
// 	collector.durationStats.update(float64(duration))
// }

// // MeanTime calculates call mean time
// func (collector *CallStatsCollector) MeanTime() time.Duration {
// 	return time.Duration(collector.durationStats.mean())
// }

// // ErrorRate calculates error rate
// func (collector *CallStatsCollector) ErrorRate() float64 {
// 	return 1 - collector.errorRates.mean()
// }

// // durationStats keeps track on call duration and error rate
// type dataSeries struct {
// 	chunks       []float64
// 	size         int
// 	nextPosition int
// }

// func (dStats *dataSeries) update(value float64) {
// 	if dStats.size == 0 || len(dStats.chunks) < dStats.size {
// 		dStats.chunks = append(dStats.chunks, value)
// 		return
// 	}
// 	dStats.chunks[dStats.nextPosition] = value
// 	dStats.nextPosition = (dStats.nextPosition + 1) % dStats.size
// }

// func (dStats *dataSeries) mean() float64 {
// 	var sum float64
// 	for _, v := range dStats.chunks {
// 		sum += v
// 	}
// 	return sum / float64(len(dStats.chunks))
// }
