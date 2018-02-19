package storages

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockedRoundTripper struct {
	mock.Mock
	http.RoundTripper
}

func (mockedRoundTripper *MockedRoundTripper) RoundTrip(request *http.Request) (response *http.Response, err error) {

	args := mockedRoundTripper.Called(request)

	return args.Get(0).(*http.Response), args.Error(1)
}

func TestShouldReturnEmptyRingAndEmptyHostnameListWhenProvidedBackendListIsEmpty(testSuite *testing.T) {

	fallbackRoundTripper := &MockedRoundTripper{}

	clusterToSetup := &Cluster{
		transport: fallbackRoundTripper,
		backends: []http.RoundTripper{},
		name: "some-cluster",
		Logger: nil,
		MethodSet: nil,
		respHandler: nil,
	}


	multiPartRoundTripper := NewMultiPartRoundTripper(clusterToSetup, nil)

	assert.Equal(testSuite, multiPartRoundTripper.activeBackendsRing.Size(), 0)
	assert.Empty(testSuite, multiPartRoundTripper.hostsToSync)
}

func TestShouldReturnMultiPartUploadRingAndEmptyHostnamesToSyncListWhenOnlyOneActiveBackendIsProvided(testSuite *testing.T) {

	fallbackRoundTripper := &MockedRoundTripper{}

	backend := &Backend{
		RoundTripper: &MockedRoundTripper{},
		Endpoint:     url.URL{},
		Maintenance:  false,
		Name:         "someBackend",
	}

	clusterToSetup := &Cluster{
		transport: fallbackRoundTripper,
		backends: []http.RoundTripper{backend},
		name: "some-cluster",
		Logger: nil,
		MethodSet: nil,
		respHandler: nil,
	}

	multiPartRoundTripper := NewMultiPartRoundTripper(clusterToSetup, nil)

	backendFromRing, _ := multiPartRoundTripper.activeBackendsRing.GetNode("arbitraryKey")

	assert.Equal(testSuite, multiPartRoundTripper.activeBackendsRing.Size(), 1)
	assert.Equal(testSuite, backendFromRing, backend.Name)
	assert.Empty(testSuite, multiPartRoundTripper.hostsToSync)
}

func TestShouldMarkBackendToBeSynchronizedWhenItIsInMaintenanceMode(testSuite *testing.T) {

	fallbackRoundTripper := &MockedRoundTripper{}

	activateBackend := &Backend{
		RoundTripper: nil,
		Endpoint:     url.URL{},
		Maintenance:  false,
		Name:         "activateBackend",
	}

	maintenanceBackendUrl, _ := url.Parse("http://maintenance:8421")

	maintenanceBackend := &Backend{
		RoundTripper: nil,
		Endpoint:     *maintenanceBackendUrl,
		Maintenance:  true,
		Name:         "maintenanceBackend",
	}

	clusterToSetup := &Cluster{
		transport: fallbackRoundTripper,
		backends: []http.RoundTripper{activateBackend, maintenanceBackend},
		name: "some-cluster",
		Logger: nil,
		MethodSet: nil,
		respHandler: nil,
	}

	multiPartRoundTripper := NewMultiPartRoundTripper(clusterToSetup, nil)

	backendFromRing, _ := multiPartRoundTripper.activeBackendsRing.GetNode("arbitraryKey")

	assert.Len(testSuite, multiPartRoundTripper.hostsToSync, 1)
	assert.Equal(testSuite, multiPartRoundTripper.hostsToSync[0], maintenanceBackend.Endpoint.String())
	assert.Equal(testSuite, multiPartRoundTripper.activeBackendsRing.Size(), 1)
	assert.Equal(testSuite, backendFromRing, activateBackend.Name)
}
