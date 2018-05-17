package storages

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestObjectResponsePickerAllGood(t *testing.T) {
	responsesChan := createChanOfResponses(true, true, true)

	objResponsePicker := &ObjectResponsePicker{responsesChan: responsesChan}
	resp, err := objResponsePicker.Pick()
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.True(t, resp.StatusCode < 400)
}

func TestObjectResponsePickerSomeGood(t *testing.T) {
	responsesSeries := [][]bool{
		{true, true, false},
		{true, false, true},
		{true, false, false},
		{false, true, true},
		{false, false, true},
		{false, true, false},
	}

	for _, serie := range responsesSeries {
		responsesChan := createChanOfResponses(serie...)
		objResponsePicker := &ObjectResponsePicker{responsesChan: responsesChan}
		resp, err := objResponsePicker.Pick()
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.True(t, resp.StatusCode < 400)
	}
}

func TestObjectResponsePickerAllBad(t *testing.T) {
	responsesChan := createChanOfResponses(false, false, false)

	objResponsePicker := &ObjectResponsePicker{responsesChan: responsesChan}
	resp, err := objResponsePicker.Pick()
	require.Error(t, err)
	require.Nil(t, resp)
}

func createChanOfResponses(successful ...bool) chan BackendResponse {
	backendResponses := []BackendResponse{}
	for _, good := range successful {
		resp := BackendResponse{
			Error:    fmt.Errorf("someerror"),
			Response: nil,
		}
		if good {
			resp = BackendResponse{
				Error:    nil,
				Response: &http.Response{StatusCode: 200},
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
