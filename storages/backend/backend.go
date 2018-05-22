package backend

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/allegro/akubra/log"
	"github.com/allegro/akubra/metrics"
	"github.com/allegro/akubra/types"
)

// Backend represents any storage in akubra cluster
type Backend struct {
	http.RoundTripper
	Endpoint    url.URL
	Name        string
	Maintenance bool
}

// RoundTrip satisfies http.RoundTripper interface
func (b *Backend) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	defer b.collectMetrics(resp, err, time.Now())
	req.URL.Host = b.Endpoint.Host
	req.URL.Scheme = b.Endpoint.Scheme

	reqID := req.Context().Value(log.ContextreqIDKey)

	if b.Maintenance {
		log.Debugf("Request %s blocked %s is in maintenance mode", reqID, req.URL.Host)
		return nil, &types.BackendError{HostName: b.Endpoint.Host,
			OrigErr: types.ErrorBackendMaintenance}
	}

	resp, oerror := b.RoundTripper.RoundTrip(req)

	if oerror != nil {
		err = &types.BackendError{HostName: b.Endpoint.Host, OrigErr: oerror}
	}

	return resp, err
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
	Error    error
	Backend  *Backend
}

// DiscardBody drain and close response Body, so connections are properly closed
func (br *Response) DiscardBody() error {
	if br.Response == nil || br.Response.Body == nil {
		return nil
	}
	_, err := io.Copy(ioutil.Discard, br.Response.Body)
	if err != nil {
		log.Printf("Discard body error %s", err)
		return err
	}
	err = br.Response.Body.Close()
	return err
}

//IsSuccessful returns true if no networ error occured and status code < 400
func (br *Response) IsSuccessful() bool {
	return br.Error == nil && br.Response != nil && br.Response.StatusCode < 400
}
