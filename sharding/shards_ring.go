package sharding

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/allegro/akubra/log"
	"github.com/allegro/akubra/metrics"
	"github.com/allegro/akubra/storages"
	"github.com/allegro/akubra/types"
	"github.com/serialx/hashring"
)

const (
	noTimeoutRegressionHeader = "X-Akubra-No-Regression-On-Failure"
)

// ShardsRingAPI interface
type ShardsRingAPI interface {
	DoRequest(req *http.Request) (resp *http.Response, rerr error)
}

// ShardsRing implements http.RoundTripper interface,
// and directs requests to determined shard
type ShardsRing struct {
	ring                    *hashring.HashRing
	shardClusterMap         map[string]storages.NamedShardClient
	allClustersRoundTripper http.RoundTripper
	clusterRegressionMap    map[string]storages.NamedShardClient
	inconsistencyLog        log.Logger
}

func (sr ShardsRing) isBucketPath(path string) bool {
	trimmedPath := strings.Trim(path, "/")
	return len(strings.Split(trimmedPath, "/")) == 1
}

// Pick finds cluster for given relative uri
func (sr ShardsRing) Pick(key string) (storages.NamedShardClient, error) {
	var shardName string

	shardName, ok := sr.ring.GetNode(key)
	if !ok {
		return &storages.ShardClient{}, fmt.Errorf("no shard for key %s", key)
	}
	shardCluster, ok := sr.shardClusterMap[shardName]
	if !ok {
		return &storages.ShardClient{}, fmt.Errorf("no cluster for shard %s, cannot handle key %s", shardName, key)
	}

	return shardCluster, nil
}

type reqBody struct {
	bytes []byte
	r     io.Reader
}

func (rb *reqBody) Reset() io.ReadCloser {
	return &reqBody{bytes: rb.bytes}
}

func (rb *reqBody) Read(b []byte) (int, error) {
	if rb.r == nil {
		rb.r = bytes.NewBuffer(rb.bytes)
	}
	return rb.r.Read(b)
}

func (rb *reqBody) Close() error {
	return nil
}

func copyRequest(origReq *http.Request) (*http.Request, error) {
	newReq := new(http.Request)
	*newReq = *origReq
	newReq.URL = &url.URL{}
	*newReq.URL = *origReq.URL
	newReq.Header = http.Header{}
	for k, v := range origReq.Header {
		for _, vv := range v {
			newReq.Header.Add(k, vv)
		}
	}

	if origReq.Body != nil {
		buf := &bytes.Buffer{}
		defer func() {
			err := origReq.Body.Close()
			if err != nil {
				log.Printf("Request body close error: %s", err)
			}
		}()
		n, err := io.Copy(buf, origReq.Body)
		if err != nil {
			return nil, err
		}

		if n > 0 {
			newReq.Body = &reqBody{bytes: buf.Bytes()}
		} else {
			newReq.Body = nil
		}
		newReq.ContentLength = int64(buf.Len())
	}
	return newReq, nil
}

func (sr ShardsRing) send(roundTripper http.RoundTripper, req *http.Request) (*http.Response, error) {
	// Rewind request body
	bodyResetter, ok := req.Body.(types.Resetter)

	if ok {
		req.Body = bodyResetter.Reset()
	}
	return roundTripper.RoundTrip(req)
}

func closeBody(resp *http.Response, reqID string) {
	_, discardErr := io.Copy(ioutil.Discard, resp.Body)
	if discardErr != nil {
		log.Printf("Cannot discard response body for req %s, reason: %q",
			reqID, discardErr.Error())
	}
	closeErr := resp.Body.Close()
	if closeErr != nil {
		log.Printf("Cannot close response body for req %s, reason: %q",
			reqID, closeErr.Error())
	}
	log.Debugf("ResponseBody for request %s closed with %s error (regression)", reqID, closeErr)
}

func (sr ShardsRing) regressionCall(cl storages.NamedShardClient, origClusterName string, req *http.Request) (string, *http.Response, error) {
	resp, err := sr.send(cl, req)
	// Do regression call if response status is > 400
	if shouldCallRegression(req, resp, err) {
		rcl, ok := sr.clusterRegressionMap[cl.Name()]
		if ok && rcl.Name() != origClusterName {
			if resp != nil && resp.Body != nil {
				reqID, _ := req.Context().Value(log.ContextreqIDKey).(string)
				closeBody(resp, reqID)
			}
			return sr.regressionCall(rcl, origClusterName, req)
		}
	}
	return cl.Name(), resp, err
}

func shouldCallRegression(request *http.Request, response *http.Response, err error) bool {
	if err == nil && response != nil {
		return (response.StatusCode > 400) && (response.StatusCode < 500)
	}
	if _, hasHeader := request.Header[noTimeoutRegressionHeader]; !hasHeader {
		return true
	}
	return false
}

func (sr *ShardsRing) logInconsistency(key, expectedClusterName, actualClusterName string) {
	logJSON, err := json.Marshal(
		struct {
			Key      string
			Expected string
			Actual   string
		}{key, expectedClusterName, actualClusterName})
	if err == nil {
		sr.inconsistencyLog.Printf(fmt.Sprintf("%s", logJSON))
	}
}

// DoRequest performs http requests to all backends that should be reached within this shards ring and with given method
func (sr ShardsRing) DoRequest(req *http.Request) (resp *http.Response, rerr error) {
	since := time.Now()
	defer func() {
		metrics.UpdateSince("reqs.global.all", since)
		if rerr != nil {
			metrics.UpdateSince("reqs.global.err", since)
		}
		if resp != nil {
			name := fmt.Sprintf("reqs.global.status_%d", resp.StatusCode)
			metrics.UpdateSince(name, since)
		}
		if req != nil {
			methodName := fmt.Sprintf("reqs.global.method_%s", req.Method)
			metrics.UpdateSince(methodName, since)
		}
	}()

	reqCopy, err := copyRequest(req)
	if err != nil {
		return nil, err
	}

	isBucketReq := sr.isBucketPath(reqCopy.URL.Path)

	if reqCopy.Method == http.MethodDelete || isBucketReq {
		return sr.allClustersRoundTripper.RoundTrip(reqCopy)
	}

	cl, err := sr.Pick(reqCopy.URL.Path)
	if err != nil {
		return nil, err
	}

	clusterName, resp, err := sr.regressionCall(cl, cl.Name(), reqCopy)
	if (clusterName != cl.Name()) && (reqCopy.Method == http.MethodPut) {
		sr.logInconsistency(reqCopy.URL.Path, cl.Name(), clusterName)
	}

	return resp, err
}
