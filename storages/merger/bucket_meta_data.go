package merger

import (
	"bytes"
	"io/ioutil"
	"net/http"

	"github.com/allegro/akubra/log"

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
	resp := firstResponse.Response
	firstResponseBodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Could not read first response body reqID %s, reason %s", firstResponse.ReqID(), err)
	}
	for _, bresp := range successes {
		responseBodyBytes, readErr := ioutil.ReadAll(bresp.Response.Body)
		if readErr != nil || !bytes.Equal(firstResponseBodyBytes, responseBodyBytes) {
			resp.Header.Set(warningHeader, inconsitentRespInfo)
		}
		err = bresp.DiscardBody()
		if err != nil {
			log.Printf("Cannot close success response body reqId %s, reason: %s", firstResponse.ReqID(), err)
		}
	}

	resp.Body = ioutil.NopCloser(bytes.NewReader(firstResponseBodyBytes))

	return resp, firstResponse.Error
}
