package storages

import (
	"net/http"
)

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

	return
}
