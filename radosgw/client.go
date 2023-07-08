package radosgw

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/regenttsui/s3box"
	"net/http"
	"strings"
	"time"
)

type RgwClient struct {
	config *aws.Config
}

func NewRgwClient(conf *aws.Config) *RgwClient {
	*conf.Endpoint = strings.TrimRight(*conf.Endpoint, "/")
	client := &RgwClient{config: conf}
	return client
}

func (rgw *RgwClient) buildSignerV2AndSendReq(req *http.Request) (*http.Response, error) {
	signer := s3box.NewSigner(*rgw.config, time.Now())
	err := signer.Sign(req)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)

	return resp, err
}
