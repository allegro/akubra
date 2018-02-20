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

func TestShouldReturnEmptyRingWhenProvidedBackendListIsEmpty(testSuite *testing.T) {

	fallbackRoundTripper := &MockedRoundTripper{}

	clusterToSetup := &Cluster{
		transport:   fallbackRoundTripper,
		backends:    []http.RoundTripper{},
		name:        "some-cluster",
		Logger:      nil,
		MethodSet:   nil,
		respHandler: nil,
	}

	multiPartRoundTripper := NewMultiPartRoundTripper(clusterToSetup, nil)

	assert.Equal(testSuite, multiPartRoundTripper.backendsRing.Size(), 0)
	assert.Empty(testSuite, multiPartRoundTripper.backendsEndpoints, 0)
}

func TestShouldSetupMultiUploadRingAndMigrationEndpoints(testSuite *testing.T) {

	fallbackRoundTripper := &MockedRoundTripper{}

	activeBackendRoundTripper := &MockedRoundTripper{}
	activeBackendRoundTripper2 := &MockedRoundTripper{}

	activeBackendURL, _ := url.Parse("http://backend:1234")
	activeBackendURL2, _ := url.Parse("http://backend2:1234")

	activateBackend := &Backend{
		RoundTripper: activeBackendRoundTripper,
		Endpoint:     *activeBackendURL,
		Maintenance:  false,
		Name:         "activateBackend",
	}

	activateBackend2 := &Backend{
		RoundTripper: activeBackendRoundTripper2,
		Endpoint:     *activeBackendURL2,
		Maintenance:  false,
		Name:         "activateBackend2",
	}

	maintenanceBackendURL, _ := url.Parse("http://maintenance:8421")

	maintenanceBackend := &Backend{
		RoundTripper: nil,
		Endpoint:     *maintenanceBackendURL,
		Maintenance:  true,
		Name:         "maintenanceBackend",
	}

	clusterToSetup := &Cluster{
		transport:   fallbackRoundTripper,
		backends:    []http.RoundTripper{activateBackend, activateBackend2, maintenanceBackend},
		name:        "some-cluster",
		Logger:      nil,
		MethodSet:   nil,
		respHandler: nil,
	}

	multiPartRoundTripper := NewMultiPartRoundTripper(clusterToSetup, nil)

	assert.Len(testSuite, multiPartRoundTripper.backendsRoundTrippers, 2)
	assert.Equal(testSuite, multiPartRoundTripper.backendsRing.Size(), 3)
	assert.Len(testSuite, multiPartRoundTripper.backendsEndpoints, 3)
}
