package s3

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"testing"

	"fmt"

	"github.com/AdRoll/goamz/s3"
	"github.com/allegro/akubra/internal/brim/model"
	"github.com/stretchr/testify/assert"
)

func TestShouldExtractContentTypeAndLength(t *testing.T) {
	expectedContentType := "text/html"
	expectedContentLength := int64(10)
	headers := http.Header{
		"Content-Type":   {"text/html"},
		"Content-Length": {"10"},
	}
	contentType, contentLength, err := extractContentTypeAndLength(headers, false)
	assert.Equal(t, expectedContentType, contentType)
	assert.Equal(t, expectedContentLength, contentLength)
	assert.Nil(t, err)
}

func TestShouldNotExtractContentTypeAndLengthWithEmptyContentLength(t *testing.T) {
	headers := http.Header{
		"Content-Type":   {"text/html"},
		"Content-Length": {""},
	}
	_, _, err := extractContentTypeAndLength(headers, false)
	assert.Equal(t, err, ErrZeroContentLenthValue)
}

func TestShouldPrepareMetadataAndHeadersFromXAMZHeaders(t *testing.T) {
	headers := http.Header{
		"Content-Type":        {"image/png"},
		"Content-Length":      {"124"},
		"X-Amz-Meta-Md5-Hash": {"e6e3b9f6f7803e6e09a47ee53064f2c5"},
	}
	expectedMeta := s3.Options{
		Meta: map[string][]string{
			"md5-hash": {"e6e3b9f6f7803e6e09a47ee53064f2c5"},
		},
	}
	inputObject := s3Object{headers: headers}

	outputObject := prepareMetadataAndHeaders(inputObject)

	assert.Equal(t, expectedMeta.Meta, outputObject.options.Meta)
}

func TestShouldPrepareMetadataAndHeadersWithRequiredHeaders(t *testing.T) {
	headers := http.Header{
		"Accept-Ranges":  {"bytes"},
		"Etag":           {"413343bafea650838e4b7b8da31f960c"},
		"Content-Type":   {"image/jpeg"},
		"Content-Length": {"12345"},
		"Cache-Control":  {"public, s-maxage=600, max-age=600"},
		"Date":           {"Fri, 18 Aug 2017 13:03:33 GMT"},
	}
	expectedHeaders := http.Header{
		"Content-Type":   {"image/jpeg"},
		"Content-Length": {"12345"},
		"Date":           {"Fri, 18 Aug 2017 13:03:33 GMT"},
	}

	inputObject := s3Object{headers: headers}

	outputObject := prepareMetadataAndHeaders(inputObject)

	assert.Equal(t, len(expectedHeaders), len(outputObject.headers))

	for headerKey, headerValue := range expectedHeaders {
		assert.Equal(t, outputObject.headers[headerKey], headerValue)
	}
}

func TestPrepareMetadataAndHeadersWithBody(t *testing.T) {
	expectedPayload := []byte{'t', 'e', 's', 't'}
	headers := http.Header{
		"Content-Type":   {"text/html"},
		"Content-Length": {fmt.Sprintf("%d", len(expectedPayload))},
		"Date":           {"Fri, 18 Aug 2017 16:34:44 GMT"},
	}
	inputObject := s3Object{headers: headers, data: ioutil.NopCloser(bytes.NewBuffer(expectedPayload))}

	outputObject := prepareMetadataAndHeaders(inputObject)
	outputObjectPayload, err := ioutil.ReadAll(outputObject.data)
	assert.NoError(t, err)
	assert.Equal(t, expectedPayload, outputObjectPayload)
}

func TestShouldSetRetryableToTrueInMigrationTaskResultForEmptyError(t *testing.T) {
	var err error

	mtr := &MigrationTaskResult{}
	mtr.MarkRetry(model.SourceError, err)

	assert.True(t, mtr.Retryable)
}

func TestShouldSetRetryableToFalseInMigrationTaskResultForErrEmptyContentType(t *testing.T) {
	err := ErrEmptyContentType

	mtr := &MigrationTaskResult{}
	mtr.MarkRetry(model.SourceError, err)

	assert.False(t, mtr.Retryable)
}

func TestShouldSetRetryableToFalseInMigrationTaskResultForErrZeroContentLenthValue(t *testing.T) {
	err := ErrZeroContentLenthValue

	mtr := &MigrationTaskResult{}
	mtr.MarkRetry(model.SourceError, err)

	assert.False(t, mtr.Retryable)
}

func TestShouldSetRetryableToFalseInMigrationTaskResultForHttpStatusNotFound(t *testing.T) {
	err := &s3.Error{StatusCode: http.StatusNotFound}

	mtr := &MigrationTaskResult{}
	mtr.MarkRetry(model.SourceError, err)

	assert.False(t, mtr.Retryable)
}

type aclTestCase struct {
	ACLMode           model.ACLMode
	ExpectedObjectACL s3.ACL
	SourceObjectACL   s3.ACL
}

func TestShouldSetACLAccordingToTheACLModeSpecifiedInTheTask(t *testing.T) {
	aclTestCases := []aclTestCase{
		{model.ACLCopyFromSource, s3.PublicRead, s3.PublicRead},
		{model.ACLNone, s3.Private, s3.PublicRead},
	}

	for _, testCase := range aclTestCases {
		taskMigrator := TaskMigrator{
			Task: MigrationTaskData{
				srcBucketName: "srcBucket",
				dstBucketName: "dstBucket",
				aclMode:       testCase.ACLMode,
			},
		}

		object := s3Object{perm: testCase.SourceObjectACL}
		assert.Equal(t, taskMigrator.determineACL(object), testCase.ExpectedObjectACL)
	}
}
