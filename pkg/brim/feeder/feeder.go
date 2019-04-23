package feeder

import (
	"time"
)


//ThrottledPublisherConfig is the throttler configuration
type ThrottledPublisherConfig struct {
	MaxEmittedTasksCount uint64
	TaskEmissionDuration time.Duration
	BurstEnabled		 bool
}

//Throttle throttles the channel according to the configuration
func Throttle(publisherChannel <-chan interface{}, config *ThrottledPublisherConfig) <-chan interface{} {

	throttledChannel := make(chan interface{})

	go func() {

		emissionStart := time.Now()
		emittedItemsCount := uint64(0)
		emissionDelay := time.Duration(config.TaskEmissionDuration.Nanoseconds() / int64(config.MaxEmittedTasksCount))

		for {
			next := <-publisherChannel

			if config.BurstEnabled && emittedItemsCount + 1 >= config.MaxEmittedTasksCount {
				nextEmissionDelay := time.Until(emissionStart.Add(config.TaskEmissionDuration))
				time.Sleep(nextEmissionDelay)
				emittedItemsCount = 0
				emissionStart = time.Now()
			}

			if !config.BurstEnabled {
				time.Sleep(emissionDelay)
			}

			throttledChannel <- next
			emittedItemsCount++
		}
	}()

	return throttledChannel
}

