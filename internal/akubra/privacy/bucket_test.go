package privacy

import (
	"errors"
	"testing"

	"github.com/allegro/akubra/internal/akubra/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type BucketMetaDataFetcherMock struct {
	*mock.Mock
}

func TestIgnoringNonBucketRequests(t *testing.T) {
	req := requestWithBasicContext("123", "", "")
	prvContext := &Context{}

	fetcherMock := &BucketMetaDataFetcherMock{Mock: &mock.Mock{}}
	filter := NewBucketPrivacyFilter(fetcherMock)
	violation, err := filter.Filter(req, prvContext)

	assert.Equal(t, NoViolation, violation)
	assert.Nil(t, err)
}

func TestBucketFetcherFailure(t *testing.T) {
	bucketName := "bucket"
	bucketLocation := &metadata.BucketLocation{Name: bucketName}
	expectedErr := errors.New("err")

	fetcherMock := &BucketMetaDataFetcherMock{Mock: &mock.Mock{}}
	fetcherMock.On("Fetch", bucketLocation).Return(nil, expectedErr)

	req := requestWithBasicContext("123", bucketName, "obj")
	prvContext := &Context{}

	filter := NewBucketPrivacyFilter(fetcherMock)
	violation, err := filter.Filter(req, prvContext)

	assert.Equal(t, NoViolation, violation)
	assert.NotNil(t, err)
}

func TestBucketMetaDataNotFound(t *testing.T) {
	bucketName := "bucket"
	bucketLocation := &metadata.BucketLocation{Name: bucketName}

	fetcherMock := &BucketMetaDataFetcherMock{Mock: &mock.Mock{}}
	fetcherMock.On("Fetch", bucketLocation).Return(nil, nil)

	req := requestWithBasicContext("123", bucketName, "obj")
	prvContext := &Context{}

	filter := NewBucketPrivacyFilter(fetcherMock)
	violation, err := filter.Filter(req, prvContext)

	assert.Equal(t, NoViolation, violation)
	assert.Nil(t, err)
}

func TestRestrictingAccessToInternalBucket(t *testing.T) {
	bucketName := "bucket"
	bucketLocation := &metadata.BucketLocation{Name: bucketName}

	for _, isBucketInternal := range []bool{true, false} {
		for _, isRequestInternal := range []bool{true, false} {

			bucketMetaData := metadata.BucketMetaData{
				Name:       bucketName,
				IsInternal: isBucketInternal,
			}

			fetcherMock := &BucketMetaDataFetcherMock{Mock: &mock.Mock{}}
			fetcherMock.On("Fetch", bucketLocation).Return(&bucketMetaData, nil)
			req := requestWithBasicContext("123", bucketName, "obj")

			prvContext := &Context{isInternalNetwork: isRequestInternal}

			filter := NewBucketPrivacyFilterFunc(fetcherMock)
			violation, err := filter(req, prvContext)

			assert.Nil(t, err)

			if isBucketInternal && !isRequestInternal {
				assert.Equal(t, InternalNetworkBucket, violation)
			} else {
				assert.Equal(t, NoViolation, violation)
			}
		}
	}
}

func TestShouldNotFetchMetadataIfTheNetworkIsInternal(t *testing.T) {
	bucketName := "bucketName"
	bucketLocation := &metadata.BucketLocation{Name: bucketName}

	fetcherMock := &BucketMetaDataFetcherMock{Mock: &mock.Mock{}}
	req := requestWithBasicContext("123", bucketName, "obj")

	prvContext := &Context{isInternalNetwork: true}

	filter := NewBucketPrivacyFilterFunc(fetcherMock)
	violation, err := filter(req, prvContext)

	assert.Nil(t, err)
	assert.Equal(t, NoViolation, violation)
	fetcherMock.AssertNotCalled(t, "Fetch", bucketLocation)
}

func (fetcher *BucketMetaDataFetcherMock) Fetch(bucketLocation *metadata.BucketLocation) (*metadata.BucketMetaData, error) {
	args := fetcher.Called(bucketLocation)
	var metaData *metadata.BucketMetaData
	if args.Get(0) != nil {
		metaData, _ = args.Get(0).(*metadata.BucketMetaData)
	}
	return metaData, args.Error(1)
}
