package radosgw

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/regenttsui/s3box"
	"net/http"
	"strings"
	"time"
)

type RGWClient struct {
	config *aws.Config
}

func NewRGWClient(conf *aws.Config) *RGWClient {
	*conf.Endpoint = strings.TrimRight(*conf.Endpoint, "/")
	client := &RGWClient{config: conf}
	return client
}

func (rgw *RGWClient) buildSignerV2AndSendReq(req *http.Request) (*http.Response, error) {
	signer := s3box.NewSigner(*rgw.config, time.Now())
	err := signer.Sign(req)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)

	return resp, err
}
