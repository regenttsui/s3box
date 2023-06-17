package s3box

import (
	"github.com/aws/aws-sdk-go/service/s3"
)

type BoxClient struct {
	svc *s3.S3
}

func NewBoxClient(svc *s3.S3) *BoxClient {
	client := &BoxClient{svc: svc}
	return client
}
