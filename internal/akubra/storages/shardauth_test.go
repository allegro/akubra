package storages

import (
	"github.com/allegro/akubra/internal/akubra/httphandler"
	"github.com/allegro/akubra/internal/akubra/log"
	"github.com/allegro/akubra/internal/akubra/storages/auth"
	"github.com/allegro/akubra/internal/akubra/storages/config"
	"github.com/allegro/akubra/internal/akubra/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/wookie41/minio-go/pkg/s3signer"
	"golang.org/x/net/context"
	"net/http"
	"testing"
)

type shardClientMock struct {
	*mock.Mock
}

func TestShouldNotAttemptToAuthorizeRequestWithoutAuthHeader(t *testing.T) {
	reqWithoutAuthHeader, _ := http.NewRequest(http.MethodGet, "http://localhost:8080/bucket/obj", nil)
	expectedResp := http.Response{Request: reqWithoutAuthHeader, StatusCode: http.StatusOK}

	shardMock := shardClientMock{Mock: &mock.Mock{}}
	shardMock.On("RoundTrip", reqWithoutAuthHeader).Return(&expectedResp, nil)

	shardAuthenticator := NewShardAuthenticator(&shardMock)
	resp, err := shardAuthenticator.RoundTrip(reqWithoutAuthHeader)

	assert.Nil(t, err)
	assert.Equal(t, &expectedResp, resp)
}


func TestShouldReturnAccessDeniedWhenCredentialsDoNotMatch(t *testing.T) {
	access := "access"
	secret := "secret"

	passthroughBackend := StorageClient{Storage: config.Storage{Type: auth.Passthrough},}
	fixedKeyBackend := StorageClient{Storage: config.Storage{
		Type: auth.S3FixedKey,
		Properties: map[string]string{"AccessKey": access, "Secret": secret}},}

	req, _ := http.NewRequest(http.MethodGet, "http://localhost:8080/bucket/obj", nil)
	req = s3signer.SignV2(req, access, "1234", nil)

	authHeader, err := utils.ParseAuthorizationHeader(req.Header.Get("Authorization"))
	assert.Nil(t, err)
	req = req.WithContext(context.WithValue(context.Background(), httphandler.AuthHeader, &authHeader))
	req = req.WithContext(context.WithValue(req.Context(), log.ContextreqIDKey, "123"))

	expectedResp := http.Response{Request: req, StatusCode: http.StatusOK}

	shardMock := shardClientMock{Mock: &mock.Mock{}}
	shardMock.On("RoundTrip", req).Return(&expectedResp, nil)
	shardMock.On("Backends").Return([]*StorageClient{&passthroughBackend, &fixedKeyBackend})

	shardAuthenticator := NewShardAuthenticator(&shardMock)
	resp, err := shardAuthenticator.RoundTrip(req)

	assert.Nil(t, err)
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestShouldValidateRequestCredentialsBasedOnBackendType(t *testing.T) {
	access := "access"
	secret := "secret"

	passthroughBackend := StorageClient{Storage: config.Storage{Type: auth.Passthrough},}
	fixedKeyBackend := StorageClient{Storage: config.Storage{
		Type: auth.S3FixedKey,
		Properties: map[string]string{"AccessKey": access, "Secret": secret}},}

	req, _ := http.NewRequest(http.MethodGet, "http://localhost:8080/bucket/obj", nil)
	req = s3signer.SignV2(req, access, secret, nil)

	authHeader, err := utils.ParseAuthorizationHeader(req.Header.Get("Authorization"))
	assert.Nil(t, err)
	req = req.WithContext(context.WithValue(context.Background(), httphandler.AuthHeader, &authHeader))
	req = req.WithContext(context.WithValue(req.Context(), log.ContextreqIDKey, "123"))

	expectedResp := http.Response{Request: req, StatusCode: http.StatusOK}

	shardMock := shardClientMock{Mock: &mock.Mock{}}
	shardMock.On("RoundTrip", req).Return(&expectedResp, nil)
	shardMock.On("Backends").Return([]*StorageClient{&passthroughBackend, &fixedKeyBackend})

	shardAuthenticator := NewShardAuthenticator(&shardMock)
	resp, err := shardAuthenticator.RoundTrip(req)

	assert.Nil(t, err)
	assert.Equal(t, &expectedResp, resp)
}

func (mock *shardClientMock) Name() string {
	return mock.Called().String(0)
}

func (mock *shardClientMock) Backends() []*StorageClient {
	args := mock.Called()
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).([]*StorageClient)
}

func (mock *shardClientMock) RoundTrip(req *http.Request) (*http.Response, error) {
	args := mock.Called(req)
	var resp *http.Response
	if args.Get(0) != nil {
		resp = args.Get(0).(*http.Response)
	}
	return resp, args.Error(1)
}
