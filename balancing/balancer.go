package balancing

// import (
// 	"fmt"
// )

// type receiver interface {
// 	// recentTimeSpent() time.Duration
// 	// recentCalls() int
// 	recentWeight() float64
// 	isAvailable() bool
// }

// type callReceiverPicker struct {
// 	receivers []receiver
// }

// func (picker *callReceiverPicker) pickReceiver() (receiver, error) {
// 	if len(picker.receivers) == 0 {
// 		return nil, errNoAvailableReceiver
// 	}
// 	var chosen receiver
// 	for _, rec := range picker.receivers {
// 		if !rec.isAvailable() {
// 			continue
// 		}
// 		if chosen == nil {
// 			chosen = rec
// 		}
// 		if rec.recentWeight() < chosen.recentWeight() {
// 			chosen = rec
// 		}
// 	}
// 	var err error
// 	if chosen == nil {
// 		err = errNoAvailableReceiver
// 	}
// 	return chosen, err
// }

// var errNoAvailableReceiver = fmt.Errorf("No available receiver")
