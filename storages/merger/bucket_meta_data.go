package merger

import (
	"bytes"
	"io/ioutil"
	"log"
	"net/http"

	backend "github.com/allegro/akubra/storages/backend"
)

const (
	inconsitentRespInfo = "Inconsistent responses among backends"
	warningHeader       = "X-akubra-warning"
)

// MergePartially will return first success response that will have 200 or 203 status code
// to distinguish identical backend responses from differing ones.
// Applicable for ?acl ?policy ?cors ?metrics ?logging ?location ?lifecycle
func MergePartially(firstResponse backend.Response, successes []backend.Response) (*http.Response, error) {
	firstResponseBodyBytes, err := ioutil.ReadAll(firstResponse.Response.Body)
	if err != nil {
		log.Printf("Could not read first response body reqID %s, reason %s", firstResponse.ReqID(), err)
	}
	err = firstResponse.DiscardBody()
	if err != nil {
		log.Printf("Cannot close first response body reqId %s, reason: %s", firstResponse.ReqID(), err)
	}

	resp := firstResponse.Response

	for _, bresp := range successes {
		responseBodyBytes, err := ioutil.ReadAll(bresp.Response.Body)
		if err != nil || !bytes.Equal(firstResponseBodyBytes, responseBodyBytes) {
			resp.Header.Set(warningHeader, inconsitentRespInfo)
			break
		}
	}

	resp.Body = ioutil.NopCloser(bytes.NewReader(firstResponseBodyBytes))

	return resp, firstResponse.Error
}
