package radosgw

import (
	"fmt"
	v4 "github.com/aws/aws-sdk-go/aws/signer/v4"
	"github.com/regenttsui/s3box/utils"
	"io"
	"net/http"
	"time"
)

func (rgw *RGWClient) AppendObjV2(bucketName, objKey string, position uint64, body io.ReadSeeker) (*http.Response, error) {
	url := fmt.Sprintf("%s/%s/%s?append&position=%d", *rgw.config.Endpoint, bucketName, objKey, position)
	req, err := http.NewRequest("PUT", url, body)
	if err != nil {
		return nil, err
	}

	err = utils.SetContentLengthHeader(req, body)
	if err != nil {
		return nil, err
	}

	resp, err := rgw.buildSignerV2AndSendReq(req)

	return resp, err
}

func (rgw *RGWClient) AppendObjV4(bucketName, objKey string, position uint64, body io.ReadSeeker) (*http.Response, error) {
	url := fmt.Sprintf("%s/%s/%s?append&position=%d", *rgw.config.Endpoint, bucketName, objKey, position)
	req, err := http.NewRequest("PUT", url, body)
	if err != nil {
		return nil, err
	}

	err = utils.SetContentLengthHeader(req, body)
	if err != nil {
		return nil, err
	}

	signer := v4.NewSigner(rgw.config.Credentials)
	_, err = signer.Sign(req, body, "s3", "region", time.Now())
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)

	return resp, err
}
