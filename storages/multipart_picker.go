package storages

import (
	"net/http"

	"github.com/allegro/akubra/log"
)

//PickRandomBackendForMultiPartUpload selects the backend that will handle the multi part upload requests and list of storages hostnames that later have to be synchronized
func PickRandomBackendForMultiPartUpload(backends []http.RoundTripper) (multiUploadBackend *Backend, backendsHostNamesToSync []string) {

	if len(backends) <= 0 {
		return nil, []string{}
	}

	for _, backend := range backends {

		if backend, isBackendType := backend.(*Backend); isBackendType {

			if multiUploadBackend == nil && !backend.Maintenance {
				multiUploadBackend = backend
			} else {
				backendsHostNamesToSync = append(backendsHostNamesToSync, backend.Name)
			}
		}
	}

	log.Debugf("Picked %s for multi uploads, %s to sync", multiUploadBackend.Name, backendsHostNamesToSync)

	return
}
