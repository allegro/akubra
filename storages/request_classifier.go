package storages

import "net/http"

type kind int

const (
	undefined kind = iota
	objectOp
	bucketOp
)

// DetectRequestKind returns kind
func detectRequestKind(request *http.Request) kind {
	return objectOp
}
