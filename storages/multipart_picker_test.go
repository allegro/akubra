package storages

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShouldReturnNilBackendAndEmptyHostnameListWhenProvidedBackendListIsEmpty(testSuite *testing.T) {

	backendPickedForMultiPartUpload, hostnamesToSync := PickRandomBackendForMultiPartUpload([]http.RoundTripper{})

	assert.Nil(testSuite, backendPickedForMultiPartUpload)
	assert.Empty(testSuite, hostnamesToSync)
}

func TestShouldReturnMultiPartUploadBackendAndEmptyHostnamesToSyncListWhenOnlyOneBackendIsProvided(testSuite *testing.T) {

	backend := &Backend{
		RoundTripper: nil,
		Endpoint:     url.URL{},
		Maintenance:  false,
		Name:         "someBackend",
	}

	backendPickedForMultiPartUpload, hostnamesToSync := PickRandomBackendForMultiPartUpload([]http.RoundTripper{backend})

	assert.Equal(testSuite, backend, backendPickedForMultiPartUpload)
	assert.Empty(testSuite, hostnamesToSync)
}

func TestShouldReturnMultiPartUploadBackendAndOneBackendHostnameToSync(testSuite *testing.T) {

	backend1 := &Backend{
		RoundTripper: nil,
		Endpoint:     url.URL{},
		Maintenance:  false,
		Name:         "someBackend1",
	}

	backend2 := &Backend{
		RoundTripper: nil,
		Endpoint:     url.URL{},
		Maintenance:  false,
		Name:         "someBackend2",
	}

	backendPickedForMultiPartUpload, hostnamesToSync := PickRandomBackendForMultiPartUpload([]http.RoundTripper{backend1, backend2})

	assert.Equal(testSuite, backendPickedForMultiPartUpload, backend1)
	assert.Equal(testSuite, len(hostnamesToSync), 1)
	assert.Equal(testSuite, hostnamesToSync[0], "someBackend2")
}

func TestShouldReturnMultiPartUploadBackendAndManyBackendsHostnamesToSync(testSuite *testing.T) {

	backend1 := &Backend{
		RoundTripper: nil,
		Endpoint:     url.URL{},
		Maintenance:  false,
		Name:         "someBackend1",
	}

	backend2 := &Backend{
		RoundTripper: nil,
		Endpoint:     url.URL{},
		Maintenance:  false,
		Name:         "someBackend2",
	}

	backend3 := &Backend{
		RoundTripper: nil,
		Endpoint:     url.URL{},
		Maintenance:  false,
		Name:         "someBackend2",
	}

	backendPickedForMultiPartUpload, hostnamesToSync := PickRandomBackendForMultiPartUpload([]http.RoundTripper{backend1, backend2, backend3})

	assert.Equal(testSuite, backendPickedForMultiPartUpload, backend1)
	assert.Equal(testSuite, len(hostnamesToSync), 2)
	assert.Contains(testSuite, hostnamesToSync, "someBackend2", "someBackend3")
}

func TestShouldPickTheBackendToMultiPartUploadInADeterministicWay(testSuite *testing.T) {

	backend1 := &Backend{
		RoundTripper: nil,
		Endpoint:     url.URL{},
		Maintenance:  false,
		Name:         "someBackend1",
	}

	backend2 := &Backend{
		RoundTripper: nil,
		Endpoint:     url.URL{},
		Maintenance:  false,
		Name:         "someBackend2",
	}

	backend3 := &Backend{
		RoundTripper: nil,
		Endpoint:     url.URL{},
		Maintenance:  false,
		Name:         "someBackend2",
	}

	backendPickedForMultiPartUploadOnFirstRun, hostnamesToSyncOnFirstRun := PickRandomBackendForMultiPartUpload([]http.RoundTripper{backend1, backend2, backend3})

	backendPickedForMultiPartUploadOnSecondRun, hostnamesToSyncOnSecondRun := PickRandomBackendForMultiPartUpload([]http.RoundTripper{backend1, backend2, backend3})

	assert.Equal(testSuite, backendPickedForMultiPartUploadOnFirstRun, backendPickedForMultiPartUploadOnSecondRun)
	assert.Equal(testSuite, hostnamesToSyncOnFirstRun, hostnamesToSyncOnSecondRun)

	assert.Equal(testSuite, backendPickedForMultiPartUploadOnFirstRun, backend1)
	assert.Equal(testSuite, len(hostnamesToSyncOnFirstRun), 2)
	assert.Contains(testSuite, hostnamesToSyncOnSecondRun, "someBackend2", "someBackend3")
}
