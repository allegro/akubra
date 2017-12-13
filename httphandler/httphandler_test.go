package httphandler

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/allegro/akubra/log"
	"github.com/allegro/akubra/transport"

	set "github.com/deckarep/golang-set"
	"github.com/sirupsen/logrus"
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

func mkResTuple(method, urlStr, body string, respStatus int, err error) (transport.ResErrTuple, error) {
	req, errnr := http.NewRequest(method, urlStr, ioutil.NopCloser(bytes.NewBuffer([]byte(body))))
	if errnr != nil {
		return transport.ResErrTuple{}, errnr
	}
	parentCtx := req.Context()
	req = req.WithContext(context.WithValue(parentCtx, log.ContextreqIDKey, "test"))
	failed := false
	if err != nil {
		failed = true
	}
	if respStatus > 399 {
		failed = true
	}
	res := &http.Response{Request: req, StatusCode: respStatus, ContentLength: int64(len(body))}
	return transport.ResErrTuple{Req: req, Res: res, Err: err, Failed: failed}, nil
}
func mkSuccessfulResTuples(method, urlStr string, count int) ([]transport.ResErrTuple, error) {
	tuples := make([]transport.ResErrTuple, 0)
	for i := 0; i < count; i++ {
		tup, err := mkResTuple(method, urlStr, "ok", 200, nil)
		if err != nil {
			return nil, err
		}
		tuples = append(tuples, tup)
	}
	return tuples, nil
}

func mkNetworkFailTuples(method, urlStr string, err error, count int) ([]transport.ResErrTuple, error) {
	tuples := make([]transport.ResErrTuple, 0)
	body := ioutil.NopCloser(bytes.NewBuffer([]byte("abcd")))
	req, errnr := http.NewRequest(method, urlStr, body)
	if errnr != nil {
		return nil, errnr
	}
	parentCtx := req.Context()
	req = req.WithContext(context.WithValue(parentCtx, log.ContextreqIDKey, "test"))
	errURL, parseErr := url.Parse(urlStr)
	if parseErr != nil {
		return nil, parseErr
	}
	berr := &testBackendError{err: err, backend: errURL.Host}
	for i := 0; i < count; i++ {
		tuples = append(tuples, transport.ResErrTuple{Res: nil, Req: req, Err: berr.AsError(), Failed: true})
	}
	return tuples, nil
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

type handlerTestSuite struct {
	suite.Suite
	synclogWritter *syncLogWritterStub
	synclog        *logrus.Logger
	urlStr         string
	method         string
}

func (suite *handlerTestSuite) SetupTest() {
	suite.synclogWritter = &syncLogWritterStub{Buffer: bytes.Buffer{}, called: make(chan struct{})}
	suite.synclog = &logrus.Logger{
		Out:       suite.synclogWritter,
		Level:     logrus.DebugLevel,
		Formatter: &logrus.TextFormatter{},
	}
	suite.method = "GET"
	suite.urlStr = "http://localhost:8080"

}

func (suite *handlerTestSuite) feedResponseTupleChannel(ch chan transport.ResErrTuple, tuples ...transport.ResErrTuple) {
	for _, tup := range tuples {
		ch <- tup
	}
	close(ch)
}

func (suite *handlerTestSuite) TestHandlerReturnsErrorIfAllRequestsFailed() {
	methodSet := set.NewSetWith(suite.method)
	rh := LateResponseHandler(suite.synclog, methodSet)
	ch := make(chan transport.ResErrTuple)

	tuples, err := mkNetworkFailTuples(suite.method, suite.urlStr, fmt.Errorf("Connection broken"), 2)
	suite.NoError(err, "Network Fail Tuples creation error")

	go suite.feedResponseTupleChannel(ch, tuples...)

	rtup := rh(ch)
	suite.Error(rtup.Err)
}

func (suite *handlerTestSuite) TestHandlerReturnsSuccessIfAnyResponseIsCorrect() {

	methodSet := set.NewSetWith(suite.method)
	rh := LateResponseHandler(suite.synclog, methodSet)
	ch := make(chan transport.ResErrTuple)

	tuples, err := mkNetworkFailTuples(suite.method, suite.urlStr, fmt.Errorf("Connection broken"), 2)
	suite.NoError(err, "Network Fail Tuples creation error")

	successTuples, err := mkSuccessfulResTuples(suite.method, suite.urlStr, 1)
	suite.NoError(err, "Correct Tuples creation error")

	tuples = append(tuples, successTuples...)

	go suite.feedResponseTupleChannel(ch, tuples...)

	rtup := rh(ch)
	suite.NoError(rtup.Err)
	select {
	case <-time.After(time.Second):
		suite.Fail("Not logged synclog within timeout")
	case call := <-suite.synclogWritter.called:
		suite.Zero(call)
	}
}
func TestHandlerTestSuite(t *testing.T) {
	suite.Run(t, new(handlerTestSuite))
}
