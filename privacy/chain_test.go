package privacy

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/allegro/akubra/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type chainMock struct {
	*mock.Mock
}

func TestValidationsChecking(t *testing.T) {
	for _, shouldDropOnViolation := range []bool{false, true} {
		for _, shouldDetectViolation := range []bool{false, true} {
			req := requestWithBasicContext("123", "bucket", "obj")
			expectedResponse := &http.Response{StatusCode: http.StatusOK}
			if shouldDetectViolation && shouldDropOnViolation {
				expectedResponse.StatusCode = http.StatusForbidden
			}

			chainMock := &chainMock{Mock: &mock.Mock{}}
			if shouldDetectViolation {
				chainMock.On("Filter", req).Return(InternalNetworkBucket, nil)
			} else {
				chainMock.On("Filter", req).Return(NoViolation, nil)
			}

			rtMock := &roundTripperMock{Mock: &mock.Mock{}}
			rtMock.On("RoundTrip", req).Return(expectedResponse, nil)

			chainRT := NewChainRoundTripper(shouldDropOnViolation, chainMock, rtMock)
			resp, err := chainRT.RoundTrip(req)

			assert.Nil(t, err)
			assert.Equal(t, expectedResponse.StatusCode, resp.StatusCode)

			chainMock.AssertCalled(t, "Filter", req)
			if shouldDetectViolation && shouldDropOnViolation {
				rtMock.AssertNotCalled(t, "RoundTrip", req)
			} else {
				rtMock.AssertCalled(t, "RoundTrip", req)
			}
		}
	}
}

func TestErrorHandlingDuringViolationChecking(t *testing.T) {
	for _, shouldDrop := range []bool{true, false} {
		for _, errorOccured := range []bool{true, false} {
			req := requestWithBasicContext("123", "bucket", "obj")

			var errDuringViolationChecking error
			expectedResponse := &http.Response{StatusCode: http.StatusOK}

			if errorOccured {
				errDuringViolationChecking = errors.New("some error")
			}

			chainMock := &chainMock{Mock: &mock.Mock{}}
			chainMock.On("Filter", req).Return(NoViolation, errDuringViolationChecking)

			rtMock := &roundTripperMock{Mock: &mock.Mock{}}
			rtMock.On("RoundTrip", req).Return(expectedResponse, nil)

			chainRT := NewChainRoundTripper(shouldDrop, chainMock, rtMock)
			resp, err := chainRT.RoundTrip(req)

			chainMock.AssertCalled(t, "Filter", req)
			if shouldDrop && errorOccured {
				assert.NotNil(t, err)
				assert.Nil(t, resp)
			} else {
				assert.Nil(t, err)
				assert.Equal(t, expectedResponse, resp)
			}
		}
	}
}

func TestRejectingRequestWithoutPolicyContext(t *testing.T) {
	req := requestWithBasicContext("123", "bucket", "obj")
	basicChain := NewBasicChain([]Filter{})
	violation, err := basicChain.Filter(req)
	assert.Equal(t, NoViolation, violation)
	assert.Error(t, err, ErrPrivacyContextNotPresent)
}

func TestBasicChainFiltering(t *testing.T) {
	prvCtx := &Context{}
	req := requestWithBasicContext("123", "bucket", "obj")
	req = req.WithContext(context.WithValue(req.Context(), RequestPrivacyContextKey, prvCtx))

	alwaysSuccessfulFilter := func(req *http.Request, prvCtx *Context) (ViolationType, error) {
		return NoViolation, nil
	}
	alwaysFailureFilter := func(req *http.Request, prvCtx *Context) (ViolationType, error) {
		return InternalNetworkBucket, nil
	}
	validationErr := errors.New("some err")
	alwaysErrorFilter := func(req *http.Request, prvCtx *Context) (ViolationType, error) {
		return NoViolation, validationErr
	}

	for _, testCase := range []struct {
		expectedViolation ViolationType
		expectedError     error
		filters           []Filter
	}{
		{NoViolation, nil, []Filter{alwaysSuccessfulFilter, alwaysSuccessfulFilter}},
		{InternalNetworkBucket, nil, []Filter{alwaysSuccessfulFilter, alwaysFailureFilter}},
		{InternalNetworkBucket, nil, []Filter{alwaysFailureFilter, alwaysSuccessfulFilter}},
		{InternalNetworkBucket, nil, []Filter{alwaysFailureFilter, alwaysFailureFilter}},
		{NoViolation, validationErr, []Filter{alwaysSuccessfulFilter, alwaysErrorFilter}},
		{InternalNetworkBucket, nil, []Filter{alwaysFailureFilter, alwaysErrorFilter}},
		{NoViolation, validationErr, []Filter{alwaysErrorFilter, alwaysFailureFilter}},
	} {

		basicChain := NewBasicChain(testCase.filters)
		violation, err := basicChain.Filter(req)

		assert.Equal(t, testCase.expectedViolation, violation)
		assert.Equal(t, testCase.expectedError, err)
	}
}

func requestWithBasicContext(reqID, bucket, object string) *http.Request {
	req, _ := http.NewRequest(http.MethodGet, fmt.Sprintf("http://localhost:8080/%s/%s", bucket, object), nil)
	return req.WithContext(context.WithValue(context.Background(), log.ContextreqIDKey, reqID))
}

func (cm *chainMock) Filter(req *http.Request) (ViolationType, error) {
	args := cm.Called(req)
	return args.Get(0).(ViolationType), args.Error(1)
}
