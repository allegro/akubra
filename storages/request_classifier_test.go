package storages

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestObjectRequestDetection(t *testing.T) {
	testCases := []struct {
		method   string
		url      string
		expected kind
	}{
		{"GET", "http://some.storage/bucket/object", objectOp},
		{"PUT", "http://some.storage/bucket/object", objectOp},
		{"HEAD", "http://some.storage/bucket/object", objectOp},
		{"GET", "http://some.storage/bucket", bucketOp},
		{"DELETE", "http://some.storage/bucket/object", deleteOp},
		{"POST", "http://some.storage/bucket/object?uploads", multipartOp},
		{"POST", "http://some.storage/bucket/object?uploadId=ssssss", multipartOp},
	}
	for _, testCase := range testCases {
		req, err := http.NewRequest(testCase.method, testCase.url, nil)
		require.NoError(t, err)
		kind := detectRequestKind(req)
		require.Equal(t, testCase.expected, kind)
	}

}
