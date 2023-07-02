package radosgw

import (
	"github.com/aws/aws-sdk-go/aws"
)

type RgwClient struct {
	config *aws.Config
}

func NewRgwClient(conf *aws.Config) *RgwClient {
	client := &RgwClient{config: conf}
	return client
}
