package backend

import (
	"fmt"
	"github.com/allegro/akubra/internal/akubra/storages/config"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/allegro/akubra/internal/akubra/log"
	"github.com/allegro/akubra/internal/akubra/metrics"
	"github.com/allegro/akubra/internal/akubra/types"
	"github.com/allegro/akubra/internal/akubra/utils"
)

// Backend represents any storage in akubra cluster
type Backend struct {
	config.Storage
	http.RoundTripper
	Endpoint url.URL
	Name     string
}

// RoundTrip satisfies http.RoundTripper interface
func (b *Backend) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	defer b.collectMetrics(resp, err, time.Now())
	req.URL.Host = b.Endpoint.Host
	req.URL.Scheme = b.Endpoint.Scheme

	reqID := req.Context().Value(log.ContextreqIDKey)

	if b.Maintenance {
		log.Debugf("Request %s blocked %s/%s is in maintenance mode", reqID, req.URL.Host, req.URL.Path)
		utils.SetRequestProcessingMetadata(req, "backendResponse", fmt.Sprintf("%s is in maintenance mode", req.URL.Host))
		return nil, &types.BackendError{HostName: b.Endpoint.Host,
			OrigErr: types.ErrorBackendMaintenance}
	}
	if b.BucketPrefix != "" {
		req = b.addPrefix(req)
	}
	log.Debugf("Request backend %s, %s, %s", req.URL.Host, req.URL.Path, reqID)
	resp, oerror := b.RoundTripper.RoundTrip(req)
	log.Debugf("Response error %s", oerror)

	if oerror != nil {
		err = &types.BackendError{HostName: b.Endpoint.Host, OrigErr: oerror}
		utils.SetRequestProcessingMetadata(req, "backendResponse", fmt.Sprintf("%s, err: %s", req.URL.Host, err))

	} else if resp != nil {
		log.Debugf("Body for req %s from %s%s is nil: %t, status: %d", reqID, req.URL.Host, req.URL.Path, resp.Body == nil, resp.StatusCode)
		utils.SetRequestProcessingMetadata(req, "backendResponse", fmt.Sprintf("%s, status: %d", req.URL.Host, resp.StatusCode))
	}

	return resp, err
}

func (b Backend) addPrefix(req *http.Request) *http.Request {
	path := strings.TrimPrefix(req.URL.Path, "/")
	if path != "" {
		path = strings.Join([]string{b.BucketPrefix, path}, "")
		req.URL.Path = "/" + path
	}
	return req
}

func (b *Backend) collectMetrics(resp *http.Response, err error, since time.Time) {
	metrics.UpdateSince("reqs.backend."+b.Name+".all", since)
	if err != nil {
		metrics.UpdateSince("reqs.backend."+b.Name+".err", since)
	}
	if resp != nil {
		statusName := fmt.Sprintf("reqs.backend."+b.Name+".status_%d", resp.StatusCode)
		metrics.UpdateSince(statusName, since)
		methodName := fmt.Sprintf("reqs.backend."+b.Name+".method_%s", resp.Request.Method)
		metrics.UpdateSince(methodName, since)
	}
}

// Response helps handle responses
type Response struct {
	Response *http.Response
	Request  *http.Request
	Error    error
	Backend  *Backend
}

// DiscardBody drain and close response Body, so connections are properly closed
func (br *Response) DiscardBody() error {
	backendName := "unknown"
	if br.Backend != nil {
		backendName = br.Backend.Name
	}
	if br.Response == nil || br.Response.Body == nil {
		log.Debugf("ResponseBody for request %s is nil so cannot be closed - backend: %s", br.ReqID(), backendName)
		return nil
	}
	_, err := io.Copy(ioutil.Discard, br.Response.Body)
	if err != nil {
		log.Printf("Discard body error %s", err)
	}

	err = br.Response.Body.Close()
	log.Debugf("ResponseBody for request %s closed with %s error - backend: %s", br.ReqID(), err, backendName)
	return err
}

// ReqID returns request id
func (br *Response) ReqID() string {
	if br.Request == nil {
		return ""
	}
	reqID := br.Request.Context().Value(log.ContextreqIDKey)
	if reqID == nil {
		return ""
	}
	return reqID.(string)
}

//IsSuccessful returns true if no networ error occured and status code < 400
func (br *Response) IsSuccessful() bool {
	return IsSuccessful(br.Response, br.Error)
}

//IsSuccessful returns true if no networ error occured and status code < 400
func IsSuccessful(response *http.Response, err error) bool {
	return err == nil && response != nil && response.StatusCode < 400
}
