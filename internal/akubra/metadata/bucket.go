package metadata

//BucketMetaData is akubra-specific metadata about the bucket
type BucketMetaData struct {
	//Name is the name of the bucket
	Name string
	//IsInternal tells if bucket should be accessed from internal network only
	IsInternal bool
}

//BucketLocation describes where to find the bucket
type BucketLocation struct {
	//Name is the name of the bucket
	Name string
}

//BucketMetaDataFetcher fetches metadata by BucketLocation
type BucketMetaDataFetcher interface {
	Fetch(bucketLocation *BucketLocation) (*BucketMetaData, error)
}
