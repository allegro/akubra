package merger

import (
	"bytes"
	"io/ioutil"
	"log"
	"net/http"

	backend "github.com/allegro/akubra/storages/backend"
)

// MergePartially will return first success response that will have 200 or 203 status code
// to distinguish identical backend responses from differing ones.
// Applicable for ?acl ?policy ?cors ?metrics ?logging ?location ?lifecycle
func MergePartially(firstResponse backend.Response, successes []backend.Response) (*http.Response, error) {
	firstResponseBodyBytes, err := ioutil.ReadAll(firstResponse.Response.Body)
	if err != nil {
		log.Printf("Could not read first response body reqID %s, reason %s", firstResponse.ReqID(), err)
	}
	firstResponse.DiscardBody()
	statusCode := http.StatusOK
	for _, bresp := range successes {
		responseBodyBytes, err := ioutil.ReadAll(bresp.Response.Body)
		if err != nil || !bytes.Equal(firstResponseBodyBytes, responseBodyBytes) {
			statusCode = http.StatusNonAuthoritativeInfo
			break
		}
	}
	resp := firstResponse.Response
	resp.Body = ioutil.NopCloser(bytes.NewReader(firstResponseBodyBytes))
	resp.StatusCode = statusCode
	return resp, firstResponse.Error
}
