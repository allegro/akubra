package httphandler

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldReturnEntityTooLargeCode(t *testing.T) {
	request := httptest.NewRequest("POST", "http://somepath", nil)
	request.Header.Set("Content-Length", "4096")
	handler := &Handler{bodyMaxSize: 1024, maxConcurrentRequests: 10}
	writer := httptest.NewRecorder()
	handler.ServeHTTP(writer, request)
	assert.Equal(t, http.StatusRequestEntityTooLarge, writer.Code)
}

func TestShouldReturnBadRequestOnUnparsableContentLengthHeader(t *testing.T) {
	request := httptest.NewRequest("POST", "http://somepath", nil)
	request.Header.Set("Content-Length", "strange-content-header")
	handler := &Handler{bodyMaxSize: 1024, maxConcurrentRequests: 10}
	writer := httptest.NewRecorder()
	handler.ServeHTTP(writer, request)
	assert.Equal(t, http.StatusBadRequest, writer.Code)
}

func TestShouldReturnServiceNotAvailableOnTooManyRequests(t *testing.T) {
	request := httptest.NewRequest("GET", "http://somepath", nil)
	handler := &Handler{bodyMaxSize: 1024, maxConcurrentRequests: 0}
	writer := httptest.NewRecorder()
	handler.ServeHTTP(writer, request)
	assert.Equal(t, http.StatusServiceUnavailable, writer.Code)
}

func TestShouldReturnStatusOKOnHealthCheckEndpoint(t *testing.T) {
	expectedBody := `OK`
	expectedStatusCode := http.StatusOK
	healthCheckPath := "/status/ping"
	request := httptest.NewRequest("GET", "http://localhost"+healthCheckPath, nil)
	rt := Decorate(http.DefaultTransport, HealthCheckHandler(healthCheckPath))
	_, err := rt.RoundTrip(request)
	require.NoError(t, err)
	handler := &Handler{bodyMaxSize: 1024, maxConcurrentRequests: 1}
	writer := httptest.NewRecorder()
	handler.roundTripper = statusHandler{
		healthCheckEndpoint: healthCheckPath,
		roundTripper:        rt,
	}

	handler.ServeHTTP(writer, request)
	bodyBytes := make([]byte, 2)
	_, err = writer.Body.Read(bodyBytes)

	assert.NoError(t, err)
	bodyStr := string(bodyBytes)
	assert.Equal(t, expectedStatusCode, writer.Code)
	assert.Equal(t, expectedBody, bodyStr)
}

type syncLogWritterStub struct {
	bytes.Buffer
	called chan struct{}
}

func (s *syncLogWritterStub) Write(p []byte) (int, error) {
	s.called <- struct{}{}
	return s.Buffer.Write(p)
}

type testBackendError struct {
	backend string
	err     error
}

func (tbe *testBackendError) Backend() string {
	return tbe.backend
}

func (tbe *testBackendError) Err() error {
	return tbe.err
}

func (tbe *testBackendError) Error() string {
	return tbe.err.Error()
}
func (tbe *testBackendError) AsError() error {
	err := error(tbe)
	return err
}
