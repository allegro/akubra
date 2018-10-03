package balancing

// import (
// 	"testing"

// 	"github.com/stretchr/testify/assert"
// )

// func TestEmptyCallReceiverPicker(t *testing.T) {
// 	cr := &callReceiverPicker{}
// 	receiver, err := cr.pickReceiver()
// 	assert.Error(t, err)
// 	assert.Equal(t, errNoAvailableReceiver, err)
// 	assert.Nil(t, receiver)
// }
// func TestCallReceiverPicker(t *testing.T) {
// 	receivers := []receiver{&receiverMock{
// 		weight: 10, available: true, //duration: 10 * time.Second, numOfCalls: 1,
// 	}, &receiverMock{
// 		weight: 9, available: true, //duration: 3 * time.Second, numOfCalls: 3,
// 	}, &receiverMock{
// 		weight: 8, available: true, //duration: 2 * time.Second, numOfCalls: 4,
// 	}}
// 	cr := &callReceiverPicker{receivers: receivers}
// 	receiver, err := cr.pickReceiver()
// 	assert.NoError(t, err)
// 	assert.Equal(t, receivers[2], receiver)
// }

// func TestCallReceiverPickerOmitsUnAvialableReceivers(t *testing.T) {
// 	receivers := []receiver{&receiverMock{
// 		weight: 10, available: true, //duration: 10 * time.Second, numOfCalls: 1,
// 	}, &receiverMock{
// 		weight: 9, available: true, //duration: 3 * time.Second, numOfCalls: 3,
// 	}, &receiverMock{
// 		weight: 8, available: false, //duration: 2 * time.Second, numOfCalls: 4,
// 	}}
// 	cr := &callReceiverPicker{receivers: receivers}
// 	receiver, err := cr.pickReceiver()
// 	assert.NoError(t, err)
// 	assert.Equal(t, receivers[1], receiver)
// }

// func TestCallReceiverPickerReturnsErrorIfNoAvialableReceiver(t *testing.T) {
// 	receivers := []receiver{&receiverMock{
// 		weight: 10, available: false, //duration: 10 * time.Second, numOfCalls: 1,
// 	}, &receiverMock{
// 		weight: 9, available: false, //duration: 3 * time.Second, numOfCalls: 3,
// 	}, &receiverMock{
// 		weight: 8, available: false, //duration: 2 * time.Second, numOfCalls: 4,
// 	}}
// 	cr := &callReceiverPicker{receivers: receivers}
// 	receiver, err := cr.pickReceiver()
// 	assert.Error(t, err)
// 	assert.Equal(t, nil, receiver)
// }

// type receiverMock struct {
// 	weight float64
// 	// duration   time.Duration
// 	available bool
// 	// numOfCalls int
// }

// // func (r *receiverMock) recentTimeSpent() time.Duration {
// // 	return r.duration
// // }
// // func (r *receiverMock) recentCalls() int {
// // 	return r.numOfCalls
// // }
// func (r *receiverMock) recentWeight() float64 {
// 	return r.weight
// }

// func (r *receiverMock) isAvailable() bool {
// 	return r.available
// }
