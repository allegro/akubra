/*
 * Minio Go Library for Amazon S3 Compatible Cloud Storage
 * Copyright 2015-2017 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package s3signer

import (
	"crypto/hmac"
	"crypto/sha256"
	"net/http"

	"fmt"
	"regexp"
)

const (
	// CustomStorageHost is an internal HTTP Header that indicates a custom storage host usage
	CustomStorageHost = "X-Custom-Storage-Host-3kl1Sc29"
	// unsignedPayload - value to be set to X-Amz-Content-Sha256 header when
	unsignedPayload = "UNSIGNED-PAYLOAD"
)

// sum256 calculate sha256 sum for an input byte array.
func sum256(data []byte) []byte {
	hash := sha256.New()
	hash.Write(data)
	return hash.Sum(nil)
}

// sumHMAC calculate hmac between two input byte array.
func sumHMAC(key []byte, data []byte) []byte {
	hash := hmac.New(sha256.New, key)
	hash.Write(data)
	return hash.Sum(nil)
}

// getHostAddr returns host header if available, otherwise returns host from URL
func getHostAddr(req *http.Request) string {
	if req.Host != "" {
		return req.Host
	}
	return req.URL.Host
}

const (
	regexV2Algorithm = "AWS +(?P<access_key>[a-zA-Z0-9_-]+):(?P<signature>(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?)"
	regexV4Algorithm = "AWS4-HMAC-SHA256 +Credential=(?P<access_key>.+)/[0-9]+/(?P<region>[a-zA-Z0-9-]*)/(?P<service>[a-zA-Z0-9_-]+)/aws4_request,( +)?SignedHeaders=(?P<signed_headers>[a-z0-9-;.]+),( +)?Signature=(?P<signature>[a-z0-9]+)"
)

var reV2 = regexp.MustCompile(regexV2Algorithm)
var reV4 = regexp.MustCompile(regexV4Algorithm)

type parsedAuthorizationHeader struct {
	version       string
	accessKey     string
	signature     string
	signedHeaders string
	region        string
	service       string
}

// extractAuthorizationHeader - extract S3 authorization header details
func extractAuthorizationHeader(authorizationHeader string) (authHeader parsedAuthorizationHeader, err error) {
	if reV2.MatchString(authorizationHeader) {
		match := reV2.FindStringSubmatch(authorizationHeader)
		return parsedAuthorizationHeader{accessKey: match[1], signature: match[2], version: signV2Algorithm}, nil
	}

	if reV4.MatchString(authorizationHeader) {
		match := reV4.FindStringSubmatch(authorizationHeader)
		return parsedAuthorizationHeader{accessKey: match[1], signature: match[7], region: match[2], signedHeaders: match[5],
			version: signV4Algorithm, service: match[3]}, nil
	}

	return parsedAuthorizationHeader{}, fmt.Errorf("cannot find correct authorization header")
}
