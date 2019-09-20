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
	"sort"
	"testing"
	"net/url"
	"net/http"
	"bytes"
	"github.com/stretchr/testify/assert"
)

// Tests for 'func TestResourceListSorting(t *testing.T)'.
func TestResourceListSorting(t *testing.T) {
	sortedResourceList := make([]string, len(resourceList))
	copy(sortedResourceList, resourceList)
	sort.Strings(sortedResourceList)
	for i := 0; i < len(resourceList); i++ {
		if resourceList[i] != sortedResourceList[i] {
			t.Errorf("Expected resourceList[%d] = \"%s\", resourceList is not correctly sorted.", i, sortedResourceList[i])
			break
		}
	}
}

func TestEncodeURL2PathWithVirtualHost(t *testing.T) {

	amazonURL,  _ := url.Parse("http://my.amazon.bucket.s3.us-east-2.amazonaws.com/amazonobject")
	googleURL,  _ := url.Parse("http://my.google.bucket.storage.googleapis.com/googleobject")
	customStorageURL,  _ := url.Parse("http://my.custom.bucket.custom.storage.com/customobject")

	testCases := []struct {
		Url *url.URL
		ExpectedPath string
		CustomStorageHeader string
	} {
		{amazonURL, "/my.amazon.bucket/amazonobject", ""},
		{googleURL, "/my.google.bucket/googleobject", ""},
		{customStorageURL, "/my.custom.bucket/customobject", "custom.storage.com"},
	}

	var testResults []bool

	for _, testCase := range testCases {
		request := &http.Request{URL: testCase.Url, Header: make(map[string][]string, 1)}
		request.Header.Set(CustomStorageHost, testCase.CustomStorageHeader)
		testResult := encodeURL2Path(request) == testCase.ExpectedPath
		testResults = append(testResults, testResult)
	}

	for index, testResult := range testResults {
		if !testResult {
			t.Fatalf("Failed to translate - %s", testCases[index])
		}
	}
}

func TestShouldIngnoreTheSpecifiedHeadersDuringV2Signing(t *testing.T) {
	buf := new(bytes.Buffer)
	req := http.Request{}
	req.Header = http.Header{}
	req.Header.Add("x-amz-meta-test-header", "test-value")
	req.Header.Add("x-amz-meta-date", "123")
	writeCanonicalizedHeaders(buf, &req, map[string]bool {"x-amz-meta-test-header": true})
	assert.Equal(t, buf.String(), "x-amz-meta-date:123\n")
}