package storages

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestObjectResponsePickerAllGood(t *testing.T) {
	responsesChan := createChanOfResponses(true, true, true)

	objResponsePicker := &ObjectResponsePicker{BasePicker{responsesChan: responsesChan}, nil}
	resp, err := objResponsePicker.Pick()
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.True(t, resp.StatusCode < 400)
}

func TestObjectResponsePickerSomeGood(t *testing.T) {
	responsesSeries := [][]bool{
		{true, true, true},
		{true, true, false},
		{true, false, true},
		{true, false, false},
		{false, true, true},
		{false, false, true},
		{false, true, false},
	}

	for _, serie := range responsesSeries {
		responsesChan := createChanOfResponses(serie...)
		objResponsePicker := &ObjectResponsePicker{BasePicker{responsesChan: responsesChan}, nil}
		resp, err := objResponsePicker.Pick()
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.True(t, resp.StatusCode < 400)
	}
}

func TestObjectResponsePickerAllBad(t *testing.T) {
	responsesChan := createChanOfResponses(false, false, false)

	objResponsePicker := &ObjectResponsePicker{BasePicker{responsesChan: responsesChan}, nil}
	resp, err := objResponsePicker.Pick()
	require.Error(t, err)
	require.Nil(t, resp)
}

func TestObjectResponsePickerSingleBad(t *testing.T) {

	responsesChan := createChanOfResponses(false)

	objResponsePicker := &ObjectResponsePicker{BasePicker{responsesChan: responsesChan}, nil}
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
		delResponsePicker := &deleteResponsePicker{BasePicker{responsesChan: responsesChan}, nil, nil}
		resp, err := delResponsePicker.Pick()
		require.Error(t, err)
		require.Nil(t, resp)
	}
}

func TestDeleteResponsePickerAllGood(t *testing.T) {
	responsesChan := createChanOfResponses(true, true, true)

	delResponsePicker := &deleteResponsePicker{BasePicker{responsesChan: responsesChan}, nil, nil}
	resp, err := delResponsePicker.Pick()
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.True(t, resp.StatusCode < 400)

}

func createChanOfResponses(successful ...bool) chan BackendResponse {
	backendResponses := []BackendResponse{}
	request, _ := http.NewRequest("GET", "http://some.domain/bucket/object", nil)
	backend := &Backend{
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
			resp = BackendResponse{
				Error:    nil,
				Response: &http.Response{Request: request, ContentLength: 200, StatusCode: 200},
				Backend:  backend,
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
