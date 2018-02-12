package storages

import (
	"net/http"
	"math/rand"
	"time"
	"github.com/allegro/akubra/log"
)

func PickRandomBackendForMultiPartUpload(backends []http.RoundTripper) (multiUploadBackend *Backend, backendsHostNamesToSync []string) {

	if len(backends) <= 0 {
		return nil, []string{}
	}

	rand.Seed(time.Now().Unix())

	var multiPartUploadBackendIndex int
	alreadyPickedBackendsIndexes := make(map[int]interface{})

	for {

		if len(alreadyPickedBackendsIndexes) >= len(backends) {
			log.Debugf("Couldn't pick a backend for multipart upload from %s, all seem to be in Maintenance mode.", backends)
			return nil, []string{}
		}

		randomBackendIndex := rand.Intn(len(backends))
		randomMultiUploadBackend, isBackendType := backends[randomBackendIndex].(*Backend)

		if isBackendType && !randomMultiUploadBackend.Maintenance {
			multiPartUploadBackendIndex = randomBackendIndex
			multiUploadBackend = randomMultiUploadBackend
			break
		}

		if _, alreadyPicked := alreadyPickedBackendsIndexes[randomBackendIndex]; !alreadyPicked {
			alreadyPickedBackendsIndexes[randomBackendIndex] = nil
		}
	}

	for index, backend := range backends {
		if backend, isBackendType := backend.(*Backend); isBackendType && index != multiPartUploadBackendIndex {
			backendsHostNamesToSync = append(backendsHostNamesToSync, backend.Name)
		}
	}

	return
}
