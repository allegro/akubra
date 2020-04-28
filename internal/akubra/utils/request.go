package utils

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
)

type ContextKey string
type metadataContainer map[string][]string

var ReqMetadataKey = ContextKey("ContextReqHost")

func SetRequestProcessingMetadata(req *http.Request, key, value string) {
	requestMetadata, ok := req.Context().Value(ReqMetadataKey).(metadataContainer)
	if !ok {
		requestMetadata = make(metadataContainer)
		*req = *req.WithContext(context.WithValue(req.Context(), ReqMetadataKey, requestMetadata))
	}
	if _, ok := requestMetadata[key]; !ok {
		requestMetadata[key] = []string{}
	}
	requestMetadata[key] = append(requestMetadata[key], value)
}

func GetRequestProcessingMetadata(req *http.Request, key string) string {
	requestMetadata := req.Context().Value(ReqMetadataKey)
	reqMetaData, ok := requestMetadata.(metadataContainer)
	if !ok {
		return ""
	}
	return strings.Join(reqMetaData[key], ", ")
}

func DumpResponseBody(resp *http.Response) []byte {
	if resp.Body == nil {
		return []byte("No body")
	}
	body, err := ioutil.ReadAll(resp.Body)
	defer func(){resp.Body = ioutil.NopCloser(bytes.NewReader(body))}()
	if err != nil {
		return []byte(fmt.Sprintf("%s\nerror reading body: %s", body, err))
	}
	return body
}