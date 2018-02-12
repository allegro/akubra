package storages

import (
	"net/http"
	"github.com/stretchr/testify/assert"
	"testing"
	"net/url"
)

func TestShouldReturnNilBackendAndEmptyHostnameListWhenProvidedBackendListIsEmpty(testSuite *testing.T) {


	backendPickedForMultiPartUpload, hostnamesToSync := PickRandomBackendForMultiPartUpload([]http.RoundTripper{})

	assert.Nil(testSuite, backendPickedForMultiPartUpload)
	assert.Empty(testSuite, hostnamesToSync)
}

func TestShouldReturnNilBackendAndEmptyHostnameWhenAllBackendsAreInMaintenanceMode(testSuite *testing.T) {

	backend := &Backend{
		RoundTripper: nil,
		Endpoint: url.URL{},
		Maintenance: true,
		Name : "someBackend",
	}

	backendPickedForMultiPartUpload, hostnamesToSync := PickRandomBackendForMultiPartUpload([]http.RoundTripper{backend})

	assert.Nil(testSuite, backendPickedForMultiPartUpload)
	assert.Empty(testSuite, hostnamesToSync)
}

func TestShouldReturnMultiPartUploadBackendAndEmptyHostnamesToSyncListWhenOnlyOneBackendIsProvided(testSuite *testing.T) {

	backend := &Backend{
		RoundTripper: nil,
		Endpoint: url.URL{},
		Maintenance: false,
		Name : "someBackend",
	}

	backendPickedForMultiPartUpload, hostnamesToSync := PickRandomBackendForMultiPartUpload([]http.RoundTripper{backend})

	assert.Equal(testSuite, backend, backendPickedForMultiPartUpload)
	assert.Empty(testSuite, hostnamesToSync)
}

func TestShouldReturnMultiPartUploadBackendAndOneBackendHostnameToSync(testSuite *testing.T) {

	backend1 := &Backend{
		RoundTripper: nil,
		Endpoint: url.URL{},
		Maintenance: false,
		Name : "someBackend1",
	}

	backend2 := &Backend{
		RoundTripper: nil,
		Endpoint: url.URL{},
		Maintenance: false,
		Name : "someBackend2",
	}


	backendPickedForMultiPartUpload, hostnamesToSync := PickRandomBackendForMultiPartUpload([]http.RoundTripper{backend1, backend2})

	assert.Equal(testSuite, len(hostnamesToSync), 1)

	if hostnamesToSync[0] == "someBackend1" {
		assert.Equal(testSuite, backendPickedForMultiPartUpload, backend2)
	} else {
		assert.Equal(testSuite, backendPickedForMultiPartUpload, backend1)
	}
}

func TestShouldReturnMultiPartUploadBackendAndManyBackendsHostnamesToSync(testSuite *testing.T) {

	backend1 := &Backend{
		RoundTripper: nil,
		Endpoint: url.URL{},
		Maintenance: false,
		Name : "someBackend1",
	}

	backend2 := &Backend{
		RoundTripper: nil,
		Endpoint: url.URL{},
		Maintenance: false,
		Name : "someBackend2",
	}

	backend3 := &Backend{
		RoundTripper: nil,
		Endpoint: url.URL{},
		Maintenance: false,
		Name : "someBackend2",
	}

	backendPickedForMultiPartUpload, hostnamesToSync := PickRandomBackendForMultiPartUpload([]http.RoundTripper{backend1, backend2, backend3})

	assert.Equal(testSuite, len(hostnamesToSync), 2)

	if backendPickedForMultiPartUpload == backend1 {
		assert.Contains(testSuite, hostnamesToSync, "someBackend2", "someBackend3")
	} else if backendPickedForMultiPartUpload == backend2 {
		assert.Contains(testSuite, hostnamesToSync, "someBackend1", "someBackend3")
	} else {
		assert.Contains(testSuite, hostnamesToSync, "someBackend1", "someBackend2")
	}
}