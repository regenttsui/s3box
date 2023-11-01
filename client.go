package s3box

import (
	"github.com/aws/aws-sdk-go/service/s3"
	"net/http"
)

type BoxClient struct {
	svc        *s3.S3
	httpClient *http.Client
}

func NewBoxClient(svc *s3.S3, httpClient *http.Client) *BoxClient {
	client := &BoxClient{
		svc:        svc,
		httpClient: httpClient,
	}
	return client
}
