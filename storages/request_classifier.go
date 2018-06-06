package storages

import (
	"net/http"
	"strings"
)

type kind int

const (
	undefined kind = iota
	objectOp
	bucketOp
	deleteOp
	multipartOp
)

func detectRequestKind(request *http.Request) kind {
	if isMultiPartUploadRequest(request) {
		return multipartOp
	}
	if request.Method == http.MethodDelete {
		return deleteOp
	}
	if len(strings.Split(strings.Trim(request.URL.Path, "/"), "/")) == 1 {
		return bucketOp
	}
	return objectOp
}
