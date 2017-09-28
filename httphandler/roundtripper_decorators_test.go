package httphandler

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"testing"

	"github.com/sirupsen/logrus"

	"github.com/allegro/akubra/log"
	"github.com/stretchr/testify/assert"
)

func mkSimpleServer(t *testing.T) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headersJSON, _ := json.Marshal(r.Header)
		w.WriteHeader(http.StatusOK)
		_, err := w.Write(headersJSON)
		assert.Nil(t, err)
	}))
	return srv
}

func assertIncludeHeaders(t *testing.T, actual map[string][]string, required map[string]string) bool {
	a := assert.New(t)
	for k, v := range required {
		if values, ok := actual[textproto.CanonicalMIMEHeaderKey(k)]; ok {
			if a.Contains(values, v) {
				continue
			}
		}
		t.Errorf("Does not contain %s => %s", k, v)
		return false
	}
	return true
}

func sendReq(t *testing.T, srv *httptest.Server, method string, body io.Reader, rt http.RoundTripper) (resp *http.Response) {
	req, nreqErr := http.NewRequest(method, srv.URL, body)
	if nreqErr != nil {
		t.Errorf("Cannot create request: %q", nreqErr.Error())
	}
	res, rtErr := rt.RoundTrip(req)
	if rtErr != nil {
		t.Errorf("Cannot send request: %q", rtErr.Error())
	}
	return res
}
func TestHeadersSuplier(t *testing.T) {
	srv := mkSimpleServer(t)
	defer srv.Close()
	reqHeaders := map[string]string{"x-req": "true"}
	respHeaders := map[string]string{"x-resp": "true"}

	rt := Decorate(http.DefaultTransport, HeadersSuplier(reqHeaders, respHeaders))

	res := sendReq(t, srv, "GET", nil, rt)

	body, brErr := ioutil.ReadAll(res.Body)
	if brErr != nil {
		t.Errorf("Cannot read response: %q", brErr.Error())
	}
	receivedReqHeadersMap := make(map[string][]string)
	unmErr := json.Unmarshal(body, &receivedReqHeadersMap)
	if unmErr != nil {
		t.Errorf("Cannot parse response body: %q", unmErr.Error())
	}
	assertIncludeHeaders(t, receivedReqHeadersMap, reqHeaders)
	assertIncludeHeaders(t, map[string][]string(res.Header), respHeaders)
}

func TestOptionsHandler(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if assert.Equal(t, r.Method, "HEAD") {
			_, err := w.Write([]byte("OK"))
			assert.Nil(t, err)
			return
		}
		http.Error(w, "Unexpected method", http.StatusMethodNotAllowed)
	}))
	rt := Decorate(http.DefaultTransport, OptionsHandler)
	res := sendReq(t, srv, "OPTIONS", nil, rt)
	assert.Equal(t, http.StatusOK, res.StatusCode, "Should return ok")
}

func TestAccessLogging(t *testing.T) {
	var buf bytes.Buffer
	logger := &logrus.Logger{
		Out:       &buf,
		Formatter: log.PlainTextFormatter{},
		Hooks:     make(logrus.LevelHooks),
		Level:     logrus.DebugLevel,
	}
	rt := Decorate(http.DefaultTransport, AccessLogging(logger))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := w.Write([]byte("OK"))
		assert.Nil(t, err)
	}))

	sendReq(t, srv, "PUT", nil, rt)

	amddata := bytes.Trim(buf.Bytes(), "\n")
	amd := &AccessMessageData{}
	err := json.Unmarshal(amddata, amd)

	if err != nil {
		t.Errorf("Cannot read AccessLog message %q, %q", amddata, err)
	}
	assert.Equal(t, http.StatusOK, amd.StatusCode)
}
