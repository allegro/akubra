package httphandler

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldReturnEntityTooLargeCode(t *testing.T) {
	request := httptest.NewRequest("POST", "http://somepath", nil)
	request.Header.Set("Content-Length", "4096")
	snert := &shouldNotExecuteRoundTripper{t: t}
	rt := BodySizeLimitter(1024)(snert)
	resp, err := rt.RoundTrip(request)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusRequestEntityTooLarge, resp.StatusCode)
}

type shouldNotExecuteRoundTripper struct {
	t *testing.T
}

func (snert *shouldNotExecuteRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	snert.t.Logf("Should not be executed")
	snert.t.Fail()
	return nil, nil
}

func TestShouldReturnBadRequestOnUnparsableContentLengthHeader(t *testing.T) {
	request := httptest.NewRequest("POST", "http://somepath", nil)
	request.Header.Set("Content-Length", "strange-content-header")
	snert := &shouldNotExecuteRoundTripper{t: t}
	rt := BodySizeLimitter(1024)(snert)
	resp, err := rt.RoundTrip(request)
	assert.Nil(t, err)

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestShouldReturnServiceNotAvailableOnTooManyRequests(t *testing.T) {
	request := httptest.NewRequest("GET", "http://somepath", nil)
	snert := &shouldNotExecuteRoundTripper{t: t}
	rt := RequestLimiter(0)(snert)
	resp, err := rt.RoundTrip(request)
	assert.Nil(t, err)
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

func TestShouldReturnStatusOKOnHealthCheckEndpoint(t *testing.T) {
	expectedBody := `OK`
	expectedStatusCode := http.StatusOK
	healthCheckPath := "/status/ping"
	request := httptest.NewRequest("GET", "http://localhost"+healthCheckPath, nil)
	rt := Decorate(http.DefaultTransport, HealthCheckHandler(healthCheckPath))
	_, err := rt.RoundTrip(request)
	require.NoError(t, err)
	handler := &Handler{}
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
