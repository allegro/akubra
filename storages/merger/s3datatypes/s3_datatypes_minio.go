package s3datatypes

import (
	"fmt"
	"net/http"
	"time"
)

// ObjectInfo container for object metadata.
type ObjectInfo struct {
	// An ETag is optionally set to md5sum of an object.  In case of multipart objects,
	// ETag is of the form MD5SUM-N where MD5SUM is md5sum of all individual md5sums of
	// each parts concatenated into one string.
	ETag string `json:"etag"`

	Key          string    `json:"name"`         // Name of the object
	LastModified time.Time `json:"lastModified"` // Date and time the object was last modified.
	Size         int64     `json:"size"`         // Size in bytes of the object.
	ContentType  string    `json:"contentType"`  // A standard MIME type describing the format of the object data.

	// Collection of additional metadata on the object.
	// eg: x-amz-meta-*, content-encoding etc.
	Metadata http.Header `json:"metadata" xml:"-"`

	// Owner name.
	Owner struct {
		DisplayName string `json:"name"`
		ID          string `json:"id"`
	} `json:"owner"`

	// The class of storage used to store the object.
	StorageClass string `json:"storageClass"`

	// Error
	Err error `json:"-"`
}

func (oi ObjectInfo) String() string {
	return oi.Key
}

// CommonPrefix container for prefix response.
type CommonPrefix struct {
	Prefix string
}

func (cp CommonPrefix) String() string {
	return cp.Prefix
}

//ObjectInfos is slice of ObjectInfo
type ObjectInfos []ObjectInfo

// ToStringer returns slice of stringers
func (ois ObjectInfos) ToStringer() []fmt.Stringer {
	stringers := make([]fmt.Stringer, 0, len(ois))
	for _, item := range ois {
		stringer := fmt.Stringer(item)
		stringers = append(stringers, stringer)
	}
	return stringers
}

// FromStringer returns asserted stringer slice to ObjectInfos
func (ois ObjectInfos) FromStringer(stringers []fmt.Stringer) ObjectInfos {
	newOis := make(ObjectInfos, 0, len(stringers))
	for _, stringer := range stringers {
		item := stringer.(ObjectInfo)
		newOis = append(newOis, item)
	}
	return newOis
}

// ListBucketResult container for listObjects response.
type ListBucketResult struct {
	// A response can contain CommonPrefixes only if you have
	// specified a delimiter.
	CommonPrefixes CommonPrefixes
	// Metadata about each object returned.
	Contents  ObjectInfos
	Delimiter string

	// Encoding type used to encode object keys in the response.
	EncodingType string

	// A flag that indicates whether or not ListObjects returned all of the results
	// that satisfied the search criteria.
	IsTruncated bool
	Marker      string
	MaxKeys     int64
	Name        string

	// When response is truncated (the IsTruncated element value in
	// the response is true), you can use the key name in this field
	// as marker in the subsequent request to get next set of objects.
	// Object storage lists objects in alphabetical order Note: This
	// element is returned only if you have delimiter request
	// parameter specified. If response does not include the NextMaker
	// and it is truncated, you can use the value of the last Key in
	// the response as the marker in the subsequent request to get the
	// next set of object keys.
	NextMarker string
	Prefix     string
}

// UserInfo owner info container
type UserInfo struct {
	ID          string
	DisplayName string
}

// VersionMarker describes version entry interface
type VersionMarker interface {
	GetKey() string
	GetVersionID() string
}

// VersionInfo version item container
type VersionInfo struct {
	Key          string
	VersionID    string `xml:"VersionId"`
	IsLatest     bool
	LastModified time.Time
	ETag         string
	Size         int64
	StorageClass string
	Owner        UserInfo
}

func (vi VersionInfo) String() string {
	return vi.Key + vi.LastModified.Format(time.RFC3339)
}

// GetKey returns key to satisfy Marker interface
func (vi VersionInfo) GetKey() string {
	return vi.Key
}

// GetVersionID returns key to satisfy Marker interface
func (vi VersionInfo) GetVersionID() string {
	return vi.VersionID
}

// DeleteMarkerInfo container
type DeleteMarkerInfo struct {
	Key          string
	VersionID    string `xml:"VersionId"`
	IsLatest     bool
	LastModified time.Time
	Owner        UserInfo
}

func (dmi DeleteMarkerInfo) String() string {
	return dmi.Key + dmi.LastModified.Format(time.RFC3339)
}

// GetKey returns key to satisfy Marker interface
func (dmi DeleteMarkerInfo) GetKey() string {
	return dmi.Key
}

// GetVersionID returns key to satisfy Marker interface
func (dmi DeleteMarkerInfo) GetVersionID() string {
	return dmi.VersionID
}

// ListVersionsResult container for Bucket Object versions response
// see: https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGETVersion.html
type ListVersionsResult struct {
	Name   string
	Prefix string
	// KeyMarker Marks the last Key returned in a truncated response.
	KeyMarker string
	// VersionIDMarker Marks the last version of the Key returned in a truncated response.
	VersionIDMarker string
	MaxKeys         int64
	EncodingType    string

	// A flag that indicates whether or not ListObjects returned all of the results
	// that satisfied the search criteria.
	IsTruncated  bool
	Version      VersionInfos
	DeleteMarker DeleteMarkerInfos
	// When response is truncated (the IsTruncated element value in
	// the response is true), you can use the key name in this field
	// as marker in the subsequent request to get next set of objects.
	// Object storage lists objects in alphabetical order Note: This
	// element is returned only if you have delimiter request
	// parameter specified. If response does not include the NextMaker
	// and it is truncated, you can use the value of the last Key in
	// the response as the marker in the subsequent request to get the
	// next set of object keys.
	NextKeyMarker       string
	NextVersionIDMarker string `xml:"NextVersionIdMarker"`
}

// VersionInfos is slice []VersionInfo
type VersionInfos []VersionInfo

// ToStringer returns asserted ObjectInfos to Stringer slice
func (vi VersionInfos) ToStringer() []fmt.Stringer {
	stringers := make([]fmt.Stringer, 0, len(vi))
	for _, item := range vi {
		stringer := fmt.Stringer(item)
		stringers = append(stringers, stringer)
	}
	return stringers
}

// FromStringer returns asserted stringer slice to VersionInfos
func (vi VersionInfos) FromStringer(stringers []fmt.Stringer) VersionInfos {
	vii := make(VersionInfos, 0, len(stringers))
	for _, stringer := range stringers {
		item := stringer.(VersionInfo)
		vii = append(vii, item)
	}
	return vii
}

// DeleteMarkerInfos is slice []DeleteMarkerInfo
type DeleteMarkerInfos []DeleteMarkerInfo

// ToStringer returns asserted ObjectInfos to Stringer slice
func (dmi DeleteMarkerInfos) ToStringer() []fmt.Stringer {
	stringers := make([]fmt.Stringer, 0, len(dmi))
	for _, item := range dmi {
		stringer := fmt.Stringer(item)
		stringers = append(stringers, stringer)
	}
	return stringers
}

// FromStringer returns asserted stringer slice to DeleteMarkerInfos
func (dmi DeleteMarkerInfos) FromStringer(stringers []fmt.Stringer) DeleteMarkerInfos {
	dmii := make(DeleteMarkerInfos, 0, len(stringers))
	for _, stringer := range stringers {
		item := stringer.(DeleteMarkerInfo)
		dmii = append(dmii, item)
	}
	return dmii
}

// ListBucketV2Result container for listObjects response version 2.
type ListBucketV2Result struct {
	// A response can contain CommonPrefixes only if you have
	// specified a delimiter.
	CommonPrefixes CommonPrefixes
	// Metadata about each object returned.
	Contents  ObjectInfos
	Delimiter string

	// Encoding type used to encode object keys in the response.
	EncodingType string

	// A flag that indicates whether or not ListObjects returned all of the results
	// that satisfied the search criteria.
	IsTruncated bool
	MaxKeys     int64
	Name        string

	// Hold the token that will be sent in the next request to fetch the next group of keys
	NextContinuationToken string

	ContinuationToken string
	Prefix            string

	// FetchOwner and StartAfter are currently not used
	FetchOwner string
	StartAfter string
}

// CommonPrefixes is slice of CommonPrefix
type CommonPrefixes []CommonPrefix

// ToStringer returns asserted ObjectInfos to Stringer slice
func (cp CommonPrefixes) ToStringer() []fmt.Stringer {
	stringers := make([]fmt.Stringer, 0, len(cp))
	for _, item := range cp {
		stringer := fmt.Stringer(item)
		stringers = append(stringers, stringer)
	}
	return stringers
}

// FromStringer returns asserted stringer slice to CommonPrefixes
func (cp CommonPrefixes) FromStringer(stringers []fmt.Stringer) CommonPrefixes {
	cpp := make(CommonPrefixes, 0, len(stringers))
	for _, stringer := range stringers {
		item := stringer.(CommonPrefix)
		cpp = append(cpp, item)
	}
	return cpp
}

// ListMultipartUploadsResult decodes s3 multipart upload results
type ListMultipartUploadsResult struct {
	Bucket             string
	KeyMarker          string
	UploadIDMarker     string `xml:"UploadIdMarker"`
	NextKeyMarker      string
	NextUploadIDMarker string `xml:"NextUploadIdMarker"`
	EncodingType       string
	MaxUploads         int64
	IsTruncated        bool
	Uploads            []ObjectMultipartInfo `xml:"Upload"`
	Prefix             string
	Delimiter          string
	// A response can contain CommonPrefixes only if you specify a delimiter.
	CommonPrefixes []CommonPrefix
}

// ObjectMultipartInfo container for multipart object metadata.
type ObjectMultipartInfo struct {
	// Date and time at which the multipart upload was initiated.
	Initiated time.Time `type:"timestamp" timestampFormat:"iso8601"`

	Initiator UserInfo
	Owner     UserInfo

	// The type of storage to use for the object. Defaults to 'STANDARD'.
	StorageClass string

	// Key of the object for which the multipart upload was initiated.
	Key string

	// Size in bytes of the object.
	Size int64

	// Upload ID that identifies the multipart upload.
	UploadID string `xml:"UploadId"`

	// Error
	Err error
}
