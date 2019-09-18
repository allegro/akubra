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
	"bytes"
	"encoding/hex"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"fmt"
	"net/url"

	"github.com/allegro/akubra/external/miniotweak/s3utils"
)

// Signature and API related constants.
const (
	signV4Algorithm   = "AWS4-HMAC-SHA256"
	iso8601DateFormat = "20060102T150405Z"
	yyyymmdd          = "20060102"
)

///
/// Excerpts from @lsegal -
/// https://github.com/aws/aws-sdk-js/issues/659#issuecomment-120477258.
///
///  User-Agent:
///
///      This is ignored from signing because signing this causes
///      problems with generating pre-signed URLs (that are executed
///      by other agents) or when customers pass requests through
///      proxies, which may modify the user-agent.
///
///  Content-Length:
///
///      This is ignored from signing because generating a pre-signed
///      URL should not provide a content-length constraint,
///      specifically when vending a S3 pre-signed PUT URL. The
///      corollary to this is that when sending regular requests
///      (non-pre-signed), the signature contains a checksum of the
///      body, which implicitly validates the payload length (since
///      changing the number of bytes would change the checksum)
///      and therefore this header is not valuable in the signature.
///
///  Content-Type:
///
///      Signing this header causes quite a number of problems in
///      browser environments, where browsers like to modify and
///      normalize the content-type header in different ways. There is
///      more information on this in https://goo.gl/2E9gyy. Avoiding
///      this field simplifies logic and reduces the possibility of
///      future bugs.
///
///  Authorization:
///
///      Is skipped for obvious reasons
///
var v4IgnoredHeaders = map[string]bool{
	"Authorization":  true,
	"Content-Length": true,
	"User-Agent":     true,
	"Content-Type": true,
}

const v4SignHeadersNamePrefix = "x-amz"

// getSigningKey hmac seed to calculate final signature.
func getSigningKey(secret, loc, serv string, t time.Time) []byte {
	date := sumHMAC([]byte("AWS4"+secret), []byte(t.Format(yyyymmdd)))
	location := sumHMAC(date, []byte(loc))
	service := sumHMAC(location, []byte(serv))
	signingKey := sumHMAC(service, []byte("aws4_request"))
	return signingKey
}

// getSignature final signature in hexadecimal form.
func getSignature(signingKey []byte, stringToSign string) string {
	return hex.EncodeToString(sumHMAC(signingKey, []byte(stringToSign)))
}

// getScope generate a string of a specific date, an AWS region, and a
// service.
func getScope(location, service string, t time.Time) string {
	scope := strings.Join([]string{
		t.Format(yyyymmdd),
		location,
		service,
		"aws4_request",
	}, "/")
	return scope
}

// GetCredential generate a credential string.
func GetCredential(accessKeyID, location, service string, t time.Time) string {
	scope := getScope(location, service, t)
	return accessKeyID + "/" + scope
}

// getHashedPayload get the hexadecimal value of the SHA256 hash of
// the request payload.
func getHashedPayload(req *http.Request) string {
	hashedPayload := req.Header.Get("X-Amz-Content-Sha256")
	if hashedPayload == "" {
		// Presign does not have a payload, use S3 recommended value.
		hashedPayload = unsignedPayload
	}
	return hashedPayload
}

// getCanonicalHeaders generate a list of request headers for
// signature.
func getCanonicalHeaders(req *http.Request, ignoredHeaders map[string]bool) string {
	headers, vals := getHeadersToSign(req, ignoredHeaders)
	var buf bytes.Buffer
	// Save all the headers in canonical form <header>:<value> newline
	// separated for each header.
	for _, k := range headers {
		buf.WriteString(k)
		buf.WriteByte(':')
		switch {
		case k == "host":
			buf.WriteString(getHostAddr(req))
			fallthrough
		default:
			for idx, v := range vals[k] {
				if idx > 0 {
					buf.WriteByte(',')
				}
				buf.WriteString(v)
			}
			buf.WriteByte('\n')
		}
	}
	return buf.String()
}

// getSignedHeaders generate all signed request headers.
// i.e lexically sorted, semicolon-separated list of lowercase
// request header names.
func getSignedHeaders(req *http.Request, ignoredHeaders map[string]bool) string {
	headers,_ := getHeadersToSign(req, ignoredHeaders)
	return strings.Join(headers, ";")
}
// Returns slice of header names to be used in sign process
func getHeadersToSign(req *http.Request, ignoredHeaders map[string]bool) ([]string, map[string][]string) {
	var headers []string
	vals := make(map[string][]string)
	for k,vv := range req.Header {
		lowerCaseHeaderName := strings.ToLower(k)
		if _, ok := ignoredHeaders[http.CanonicalHeaderKey(k)]; ok {
			continue // Ignored header found continue.
		}
		if !strings.HasPrefix(lowerCaseHeaderName, v4SignHeadersNamePrefix) {
			continue
		}
		headers = append(headers, strings.ToLower(k))
		vals[strings.ToLower(k)] = vv
	}
	headers = append(headers, "host")
	sort.Strings(headers)
	return headers, vals
}

// getCanonicalRequest generate a canonical request of style.
//
// canonicalRequest =
//  <HTTPMethod>\n
//  <CanonicalURI>\n
//  <CanonicalQueryString>\n
//  <CanonicalHeaders>\n
//  <SignedHeaders>\n
//  <HashedPayload>
func getCanonicalRequest(req *http.Request, ignoredHeaders map[string]bool) string {
	req.URL.RawQuery = strings.Replace(req.URL.Query().Encode(), "+", "%20", -1)
	canonicalRequest := strings.Join([]string{
		req.Method,
		s3utils.EncodePath(req.URL.Path),
		req.URL.RawQuery,
		getCanonicalHeaders(req, ignoredHeaders),
		getSignedHeaders(req, ignoredHeaders),
		getHashedPayload(req),
	}, "\n")
	return canonicalRequest
}

// getStringToSign a string based on selected query values.
func getStringToSignV4(t time.Time, location, service, canonicalRequest string) string {
	stringToSign := signV4Algorithm + "\n" + t.Format(iso8601DateFormat) + "\n"
	stringToSign = stringToSign + getScope(location, service, t) + "\n"
	stringToSign = stringToSign + hex.EncodeToString(sum256([]byte(canonicalRequest)))
	return stringToSign
}

// PreSignV4 presign the request, in accordance with
// http://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html.
func PreSignV4(req *http.Request, accessKeyID, secretAccessKey, sessionToken, location, service string, expires int64) *http.Request {
	// Presign is not needed for anonymous credentials.
	if accessKeyID == "" || secretAccessKey == "" {
		return req
	}

	// Initial time.
	t := time.Now().UTC()

	// Get credential string.
	credential := GetCredential(accessKeyID, location, service, t)

	// Get all signed headers.
	signedHeaders := getSignedHeaders(req, v4IgnoredHeaders)

	// Set URL query.
	query := req.URL.Query()
	query.Set("X-Amz-Algorithm", signV4Algorithm)
	query.Set("X-Amz-Date", t.Format(iso8601DateFormat))
	query.Set("X-Amz-Expires", strconv.FormatInt(expires, 10))
	query.Set("X-Amz-SignedHeaders", signedHeaders)
	query.Set("X-Amz-Credential", credential)
	// Set session token if available.
	if sessionToken != "" {
		query.Set("X-Amz-Security-Token", sessionToken)
	}
	req.URL.RawQuery = query.Encode()

	// Get canonical request.
	canonicalRequest := getCanonicalRequest(req, v4IgnoredHeaders)

	// Get string to sign from canonical request.
	stringToSign := getStringToSignV4(t, location, service, canonicalRequest)

	// Gext hmac signing key.
	signingKey := getSigningKey(secretAccessKey, location, service, t)

	// Calculate signature.
	signature := getSignature(signingKey, stringToSign)

	// Add signature header to RawQuery.
	req.URL.RawQuery += "&X-Amz-Signature=" + signature

	return req
}

// PostPresignSignatureV4 - presigned signature for PostPolicy
// requests.
func PostPresignSignatureV4(policyBase64 string, t time.Time, secretAccessKey, location, service string) string {
	// Get signining key.
	signingkey := getSigningKey(secretAccessKey, location, service, t)
	// Calculate signature.
	signature := getSignature(signingkey, policyBase64)
	return signature
}

// SignV4 sign the request before Do(), in accordance with
// http://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html.
func SignV4WithIgnoredHeaders(req *http.Request, accessKeyID, secretAccessKey, sessionToken, location, service string, ignoredHeaders map[string]bool) *http.Request {
	// Signature calculation is not needed for anonymous credentials.
	if accessKeyID == "" || secretAccessKey == "" {
		return req
	}
	// Initial time.
	t := time.Now().UTC()
	// Set x-amz-date.
	sanitizeV4DateHeader(req, t)

	// Set session token if available.
	if sessionToken != "" {
		req.Header.Set("X-Amz-Security-Token", sessionToken)
	}

	// Get canonical request.
	canonicalRequest := getCanonicalRequest(req, ignoredHeaders)

	// Get string to sign from canonical request.
	stringToSign := getStringToSignV4(t, location, service, canonicalRequest)

	// Get hmac signing key.
	signingKey := getSigningKey(secretAccessKey, location, service, t)

	// Get credential string.
	credential := GetCredential(accessKeyID, location, service, t)

	// Get all signed headers.
	signedHeaders := getSignedHeaders(req, ignoredHeaders)

	// Calculate signature.
	signature := getSignature(signingKey, stringToSign)

	// If regular request, construct the final authorization header.
	parts := []string{
		signV4Algorithm + " Credential=" + credential,
		"SignedHeaders=" + signedHeaders,
		"Signature=" + signature,
	}

	// Set authorization header.
	auth := strings.Join(parts, ", ")
	req.Header.Set("Authorization", auth)

	return req
}

// SignV4 sign the request before Do(), in accordance with
// http://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html.
func SignV4(req *http.Request, accessKeyID, secretAccessKey, sessionToken, location, service string) *http.Request {
	return SignV4WithIgnoredHeaders(req, accessKeyID, secretAccessKey, sessionToken, location, service, v4IgnoredHeaders)
}

// VerifyV4 verify if v4 signature is correct
func VerifyV4(req *http.Request, secretAccessKey string) (bool, error) {
	origAuthHeader, err := extractAuthorizationHeader(req.Header.Get("Authorization"))
	if err != nil {
		return false, fmt.Errorf("error while parsing authorization header")
	}
	if origAuthHeader.version != signV4Algorithm {
		return false, fmt.Errorf("incorrect authHeader version %s", origAuthHeader.version)
	}

	// Remove x-amz-signature from url
	parsedQuery, err := url.ParseQuery(req.URL.RawQuery)
	parsedQuery.Get("x-amz-signature")
	for key := range parsedQuery {
		if strings.ToLower(key) == "x-amz-signature" {
			parsedQuery.Del(key)
		}
	}
	req.URL.RawQuery = parsedQuery.Encode()

	// Get request time from x-amz-date header
	t, err := time.Parse(iso8601DateFormat, req.Header.Get("x-amz-date"))
	if err != nil {
		return false, fmt.Errorf("error while parsing x-amz-date header")
	}

	// Get list of headers to ignore
	signedHeaders := map[string]struct{}{}
	for _, name := range strings.Split(origAuthHeader.signedHeaders, ";") {
		signedHeaders[name] = struct{}{}
	}
	ignoredHeaders := map[string]bool{}
	for name := range req.Header {
		if _, ok := signedHeaders[strings.ToLower(name)]; !ok {
			ignoredHeaders[name] = true
		}
	}

	// Get canonical request.
	canonicalRequest := getCanonicalRequest(req, ignoredHeaders)
	// Get string to sign from canonical request.
	stringToSign := getStringToSignV4(t, origAuthHeader.region, origAuthHeader.service, canonicalRequest)
	// Get hmac signing key.
	signingKey := getSigningKey(secretAccessKey, origAuthHeader.region, origAuthHeader.service, t)

	// Calculate signature.
	signature := getSignature(signingKey, stringToSign)
	if signature == origAuthHeader.signature {
		return true, nil
	}
	return false, fmt.Errorf("request signature mismatch")
}

func sanitizeV4DateHeader(req *http.Request, t time.Time) *http.Request {
	req.Header.Del("Date")
	req.Header.Set("x-amz-date", t.Format(iso8601DateFormat))
	return req
}
