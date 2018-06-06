package storages

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/allegro/akubra/storages/backend"
	"github.com/allegro/akubra/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockedRoundTripper struct {
	mock.Mock
	transport.Matcher
}

func (mockedRoundTripper *MockedRoundTripper) RoundTrip(request *http.Request) (response *http.Response, err error) {

	args := mockedRoundTripper.Called(request)

	return args.Get(0).(*http.Response), args.Error(1)
}

func TestShouldReturnEmptyRingWhenProvidedBackendListIsEmpty(testSuite *testing.T) {
	emptyBackendsList := []*backend.Backend{}
	multiPartRoundTripper := newMultiPartRoundTripper(emptyBackendsList)
	mprt, ok := multiPartRoundTripper.(*MultiPartRoundTripper)
	assert.True(testSuite, ok)
	assert.Equal(testSuite, mprt.backendsRing.Size(), 0)
	assert.Empty(testSuite, mprt.backendsEndpoints, 0)
}

func TestShouldSetupMultiUploadRingAndMigrationEndpoints(testSuite *testing.T) {

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

	backends := []*Backend{activateBackend, activateBackend2, maintenanceBackend}
	multiPartRoundTripper := newMultiPartRoundTripper(backends)
	mprt, ok := multiPartRoundTripper.(*MultiPartRoundTripper)
	assert.True(testSuite, ok)
	assert.Len(testSuite, mprt.backendsRoundTrippers, 2)
	assert.Equal(testSuite, mprt.backendsRing.Size(), 2)
	assert.Len(testSuite, mprt.backendsEndpoints, 3)
}
