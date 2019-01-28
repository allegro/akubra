package storages

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/allegro/akubra/watchdog"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestObjectResponsePickerAllGood(t *testing.T) {
	responsesChan := createChanOfResponses(true, true, true)

	objResponsePicker := &ObjectResponsePicker{BasePicker{responsesChan: responsesChan}}
	resp, err := objResponsePicker.Pick()
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.True(t, resp.StatusCode < 400)
}

func TestObjectResponsePickerSomeGood(t *testing.T) {
	for _, testScenario := range []struct {
		watchdog             *WatchdogMock
		consistencyRecord    *watchdog.ConsistencyRecord
		responses            []bool
		shouldReadRepairFail bool
	}{
		{&WatchdogMock{&mock.Mock{}}, &watchdog.ConsistencyRecord{ObjectVersion: "123"}, []bool{true, true, true}, false},
		{&WatchdogMock{&mock.Mock{}}, nil, []bool{true, true, false}, false},
		{&WatchdogMock{&mock.Mock{}}, &watchdog.ConsistencyRecord{ObjectVersion: "123"}, []bool{true, false, true}, false},
		{nil, nil, []bool{true, false, false}, false},
		{&WatchdogMock{&mock.Mock{}}, &watchdog.ConsistencyRecord{ObjectVersion: "123"}, []bool{false, true, true}, false},
		{&WatchdogMock{&mock.Mock{}}, &watchdog.ConsistencyRecord{ObjectVersion: "123"}, []bool{false, true, true}, true},
		{&WatchdogMock{&mock.Mock{}}, nil, []bool{false, false, true}, false},
		{&WatchdogMock{&mock.Mock{}}, nil, []bool{false, true, false}, false},
	} {
		responsesChan := createChanOfResponses(testScenario.responses...)
		shouldTryToInsertRecord := hasSuccessResponse(testScenario.responses) && hasFailureResponse(testScenario.responses)

		if testScenario.watchdog != nil && shouldTryToInsertRecord {
			testScenario.watchdog.On("GetVersionHeaderName").Return("x-amz-meta-version")
			if testScenario.shouldReadRepairFail {
				testScenario.watchdog.On("Insert", testScenario.consistencyRecord).Return(nil, errors.New("insert fail"))
			} else {
				testScenario.watchdog.On("Insert", testScenario.consistencyRecord).Return(nil, nil)
			}
		}

		objResponsePicker := &ObjectResponsePicker{BasePicker{responsesChan: responsesChan}}
		resp, err := objResponsePicker.Pick()
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.True(t, resp.StatusCode < 400)
		if testScenario.watchdog != nil {
			if shouldTryToInsertRecord {
				testScenario.watchdog.AssertNotCalled(t, "GetVersionHeaderName")
				testScenario.watchdog.AssertNotCalled(t, "Insert", testScenario.consistencyRecord)
			} else {
				testScenario.watchdog.AssertExpectations(t)
			}
		}
	}
}

func TestObjectResponsePickerAllBad(t *testing.T) {
	responsesChan := createChanOfResponses(false, false, false)

	objResponsePicker := &ObjectResponsePicker{BasePicker{responsesChan: responsesChan}}
	resp, err := objResponsePicker.Pick()
	require.Error(t, err)
	require.Nil(t, resp)
}

func TestObjectResponsePickerSingleBad(t *testing.T) {

	responsesChan := createChanOfResponses(false)

	objResponsePicker := &ObjectResponsePicker{BasePicker{responsesChan: responsesChan}}
	resp, err := objResponsePicker.Pick()
	require.Error(t, err)
	require.Nil(t, resp)
}

func TestDeleteResponsePickerSomeBad(t *testing.T) {
	responsesSeries := [][]bool{
		{true, true, false},
		{true, false, true},
		{true, false, false},

		{false, true, true},
		{false, false, true},
		{false, true, false},
		{false, false, false},
	}

	for _, serie := range responsesSeries {
		responsesChan := createChanOfResponses(serie...)
		delResponsePicker := &baseDeleteResponsePicker{BasePicker{responsesChan: responsesChan}, nil, pullResponses}
		resp, err := delResponsePicker.Pick()
		require.Error(t, err)
		require.Nil(t, resp)
	}
}

func TestDeleteResponsePickerAllGood(t *testing.T) {
	responsesChan := createChanOfResponses(true, true, true)

	delResponsePicker := &baseDeleteResponsePicker{BasePicker{responsesChan: responsesChan}, nil, pullResponses}
	resp, err := delResponsePicker.Pick()
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.True(t, resp.StatusCode < 400)
}

func createChanOfResponses(successful ...bool) chan BackendResponse {
	backendResponses := []BackendResponse{}
	request, _ := http.NewRequest("GET", "http://some.domain/bucket/object", nil)
	backend := &StorageClient{
		Endpoint: *request.URL,
		Name:     "somebackend",
	}
	for _, good := range successful {
		resp := BackendResponse{
			Error:    fmt.Errorf("someerror"),
			Response: nil,
			Backend:  backend,
		}
		if good {
			request, _ := http.NewRequest("PUT", "http://some.domain/bucket/key", nil)
			request.Header.Add("x-amz-meta-version", "123")
			resp = BackendResponse{
				Error:    nil,
				Response: &http.Response{Request: request, ContentLength: 200, StatusCode: 200},
				Backend:  backend,
				Request:  request,
			}
		}
		backendResponses = append(backendResponses, resp)
	}

	brespChan := make(chan BackendResponse)
	go func() {
		for _, bresp := range backendResponses {
			brespChan <- bresp
		}
		close(brespChan)
	}()
	return brespChan
}

func BenchmarkInterfaceAssertion(b *testing.B) {
	for n := 0; n < b.N; n++ {
		doInterf(n)
	}
}

func BenchmarkEmptyMethods(b *testing.B) {
	for n := 0; n < b.N; n++ {
		doEmpty(n)
	}
}

func doEmpty(i int) {
	a := &doerA{}
	b := &doerB{}
	var c doer
	if i%2 == 0 {
		c = a
	} else {
		c = b
	}
	c.do()
}

func doInterf(i int) {
	a := &doerA{}
	b := &doerC{}
	var c interface{}
	if i%2 == 0 {
		c = a
	} else {
		c = b
	}
	if cdoer, ok := c.(doer); ok {
		cdoer.do()
	}
}

type doer interface {
	do() int
}

type doerA struct{}

func (*doerA) do() int {
	i := 1
	i++
	return i
}

type doerB struct{}

func (*doerB) do() int {
	return -1
}

type doerC struct{}

func hasSuccessResponse(responses [] bool) bool {
	for _, response := range responses {
		if response {
			return true
		}
	}
	return false
}

func hasFailureResponse(responses [] bool) bool {
	for _, response := range responses {
		if !response {
			return true
		}
	}
	return false

}
