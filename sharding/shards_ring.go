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
	"github.com/serialx/hashring"
)

//ShardsRingAPI interface
type ShardsRingAPI interface {
	DoRequest(req *http.Request) (resp *http.Response, rerr error)
}

// ShardsRing implements http.RoundTripper interface,
// and directs requests to determined shard
type ShardsRing struct {
	ring                    *hashring.HashRing
	shardClusterMap         map[string]storages.Cluster
	allClustersRoundTripper http.RoundTripper
	clusterRegressionMap    map[string]storages.Cluster
	inconsistencyLog        log.Logger
}

func (sr ShardsRing) isBucketPath(path string) bool {
	trimmedPath := strings.Trim(path, "/")
	return len(strings.Split(trimmedPath, "/")) == 1
}

// Pick finds cluster for given relative uri
func (sr ShardsRing) Pick(key string) (storages.Cluster, error) {
	var shardName string

	shardName, ok := sr.ring.GetNode(key)
	if !ok {
		return storages.Cluster{}, fmt.Errorf("no shard for key %s", key)
	}
	shardCluster, ok := sr.shardClusterMap[shardName]
	if !ok {
		return storages.Cluster{}, fmt.Errorf("no cluster for shard %s, cannot handle key %s", shardName, key)
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

func (sr ShardsRing) send(roundTripper http.RoundTripper, req *http.Request) (*http.Response, error) {
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

func (sr ShardsRing) regressionCall(cl storages.Cluster, req *http.Request) (string, *http.Response, error) {
	resp, err := sr.send(cl, req)
	// Do regression call if response status is > 400
	if (err != nil || resp.StatusCode > 400) && req.Method != http.MethodPut {
		rcl, ok := sr.clusterRegressionMap[cl.Name]
		if ok {
			_, discardErr := io.Copy(ioutil.Discard, resp.Body)
			if discardErr != nil {
				reqID, _ := req.Context().Value(log.ContextreqIDKey).(string)
				log.Printf("Cannot discard response body for req %s, reason: %q",
					reqID, discardErr.Error())
			}
			closeErr := resp.Body.Close()
			if closeErr != nil {
				reqID, _ := req.Context().Value(log.ContextreqIDKey).(string)
				log.Printf("Cannot close response body for req %s, reason: %q",
					reqID, closeErr.Error())
			}
			return sr.regressionCall(rcl, req)
		}
	}
	return cl.Name, resp, err
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

//DoRequest performs http requests to all backends that should be reached within this shards ring and with given method
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

	if reqCopy.Method == http.MethodDelete || sr.isBucketPath(reqCopy.URL.Path) {
		return sr.allClustersRoundTripper.RoundTrip(reqCopy)
	}

	cl, err := sr.Pick(reqCopy.URL.Path)
	if err != nil {
		return nil, err
	}

	clusterName, resp, err := sr.regressionCall(cl, reqCopy)
	if clusterName != cl.Name {
		sr.logInconsistency(reqCopy.URL.Path, cl.Name, clusterName)
	}

	return resp, err
}
