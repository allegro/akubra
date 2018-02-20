package types

import "encoding/xml"

//CompleteMultipartUploadResult contains information about a successfull multipart upload, after the object
//has been assembled
type CompleteMultipartUploadResult struct {
	XMLName  xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ CompleteMultipartUploadResult" json:"-"`
	Location string
	Bucket   string
	Key      string
	ETag     string
}
