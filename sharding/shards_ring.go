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
	"github.com/serialx/hashring"
)

type shardsRing struct {
	ring                    *hashring.HashRing
	shardClusterMap         map[string]cluster
	allClustersRoundTripper http.RoundTripper
	clusterRegressionMap    map[string]cluster
	inconsistencyLog        log.Logger
}

func (sr shardsRing) isBucketPath(path string) bool {
	trimmedPath := strings.Trim(path, "/")
	return len(strings.Split(trimmedPath, "/")) == 1
}

func (sr shardsRing) Pick(key string) (cluster, error) {
	var shardName string

	shardName, ok := sr.ring.GetNode(key)
	if !ok {
		return cluster{}, fmt.Errorf("no shard for key %s", key)
	}
	shardCluster, ok := sr.shardClusterMap[shardName]
	if !ok {
		return cluster{}, fmt.Errorf("no cluster for shard %s, cannot handle key %s", shardName, key)
	}

	return shardCluster, nil
}

type reqBody struct {
	r *bytes.Reader
}

func (rb *reqBody) rewind() error {
	_, err := rb.r.Seek(0, io.SeekStart)
	return err
}

func (rb *reqBody) Read(b []byte) (int, error) {
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
		buf := new(bytes.Buffer)
		_, err := io.Copy(buf, origReq.Body)
		if err != nil {
			return nil, err
		}
		newReq.Body = &reqBody{bytes.NewReader(buf.Bytes())}
	}
	return newReq, nil
}

func (sr shardsRing) send(roundTripper http.RoundTripper, req *http.Request) (*http.Response, error) {
	// Rewind request body
	bodySeeker, ok := req.Body.(*reqBody)
	if ok {
		err := bodySeeker.rewind()
		if err != nil {
			return nil, err
		}
	}
	return roundTripper.RoundTrip(req)
}

func (sr shardsRing) regressionCall(cl cluster, req *http.Request) (string, *http.Response, error) {
	resp, err := sr.send(cl, req)
	// Do regression call if response status is > 400
	if (err != nil || resp.StatusCode > 400) && req.Method != http.MethodPut {
		rcl, ok := sr.clusterRegressionMap[cl.name]
		if ok {
			_, discardErr := io.Copy(ioutil.Discard, resp.Body)
			if discardErr != nil {
				reqID, _ := req.Context().Value(log.ContextreqIDKey).(string)
				log.Debugf("Cannot discard response body for req %s, reason: %q",
					reqID, discardErr.Error())
			}
			closeErr := resp.Body.Close()
			if closeErr != nil {
				reqID, _ := req.Context().Value(log.ContextreqIDKey).(string)
				log.Debugf("Cannot close response body for req %s, reason: %q",
					reqID, closeErr.Error())
			}
			return sr.regressionCall(rcl, req)
		}
	}
	return cl.name, resp, err
}
func (sr *shardsRing) logInconsistency(key, expectedClusterName, actualClusterName string) {
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

func (sr shardsRing) RoundTrip(req *http.Request) (resp *http.Response, rerr error) {
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

	if reqCopy.Method == http.MethodDelete || sr.isBucketPath(reqCopy.URL.Path) {
		return sr.allClustersRoundTripper.RoundTrip(reqCopy)
	}

	cl, err := sr.Pick(reqCopy.URL.Path)
	if err != nil {
		return nil, err
	}

	clusterName, resp, err := sr.regressionCall(cl, reqCopy)
	if clusterName != cl.name {
		sr.logInconsistency(reqCopy.URL.Path, cl.name, clusterName)
	}

	return resp, err
}
