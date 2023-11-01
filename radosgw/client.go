package radosgw

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/regenttsui/s3box"
	"net/http"
	"strings"
	"time"
)

type RGWClient struct {
	config     *aws.Config
	httpClient *http.Client
}

func NewRGWClient(conf *aws.Config, httpClient *http.Client) *RGWClient {
	*conf.Endpoint = strings.TrimRight(*conf.Endpoint, "/")
	client := &RGWClient{
		config:     conf,
		httpClient: httpClient,
	}
	return client
}

func (rgw *RGWClient) buildSignerV2AndSendReq(req *http.Request) (*http.Response, error) {
	signer := s3box.NewSigner(*rgw.config, time.Now())
	err := signer.Sign(req)
	if err != nil {
		return nil, err
	}

	resp, err := rgw.httpClient.Do(req)

	return resp, err
}
