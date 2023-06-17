package radosgw

import "github.com/aws/aws-sdk-go/service/s3"

type RgwClient struct {
	svc *s3.S3
}

func NewRgwClient(svc *s3.S3) *RgwClient {
	client := &RgwClient{svc: svc}
	return client
}
