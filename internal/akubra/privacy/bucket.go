package privacy

import (
	"fmt"
	"net/http"

	"github.com/allegro/akubra/internal/akubra/log"
	"github.com/allegro/akubra/internal/akubra/metadata"
	"github.com/allegro/akubra/internal/akubra/utils"
)

//BucketPrivacyFilter checks if any of the bucket policies are violated
type BucketPrivacyFilter struct {
	bucketMetaDataFetcher metadata.BucketMetaDataFetcher
}

//NewBucketPrivacyFilter creates an instance of BucketPrivacyFilter
func NewBucketPrivacyFilter(fetcher metadata.BucketMetaDataFetcher) *BucketPrivacyFilter {
	return &BucketPrivacyFilter{bucketMetaDataFetcher: fetcher}
}

//NewBucketPrivacyFilterFunc BucketPrivacyFilter in Filter so it can be used in Chain
func NewBucketPrivacyFilterFunc(fetcher metadata.BucketMetaDataFetcher) Filter {
	filter := NewBucketPrivacyFilter(fetcher)
	return func(req *http.Request, prvCtx *Context) (ViolationType, error) {
		return filter.Filter(req, prvCtx)
	}
}

//Filter checks for bucket-based violations
func (filter *BucketPrivacyFilter) Filter(req *http.Request, prvCtx *Context) (ViolationType, error) {
	if prvCtx.isInternalNetwork {
		return NoViolation, nil
	}

	bucketName := utils.ExtractBucketFrom(req.URL.Path)
	if bucketName == "" {
		return NoViolation, nil
	}
	reqID := utils.RequestID(req)
	log.Debugf("Asking for bucket %s metadata on reqID %s", bucketName, reqID)
	bucketLocation := metadata.BucketLocation{Name: bucketName}
	bucketMetaData, err := filter.bucketMetaDataFetcher.Fetch(&bucketLocation)
	log.Debugf("Got bucket %s metadata on reqID %s", bucketName, reqID)
	if err != nil {
		return NoViolation, fmt.Errorf("failed to verify bucket privacy, could't fetch meta data: %s", err)
	}

	if bucketMetaData == nil {
		return NoViolation, nil
	}

	if bucketMetaData.IsInternal && !prvCtx.isInternalNetwork {
		return InternalNetworkBucket, nil
	}

	return NoViolation, nil
}
