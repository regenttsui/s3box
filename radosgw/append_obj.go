package radosgw

import (
	"fmt"
	v4 "github.com/aws/aws-sdk-go/aws/signer/v4"
	"github.com/regenttsui/s3box"
	"io"
	"net/http"
	"strings"
	"time"
)

func (rgw *RgwClient) AppendObjV2(bucketName, objKey string, position uint64, body io.ReadSeeker) (*http.Response, error) {
	endpoint := strings.TrimRight(rgw.svc.Endpoint, "/")
	url := fmt.Sprintf("%s/%s/%s?append&position=%d", endpoint, bucketName, objKey, position)
	req, err := http.NewRequest("PUT", url, body)
	if err != nil {
		return nil, err
	}

	signer := s3box.NewSigner(rgw.svc.Config, time.Now())
	err = signer.Sign(req)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)

	return resp, err
}

func (rgw *RgwClient) AppendObjV4(bucketName, objKey string, position uint64, body io.ReadSeeker) (*http.Response, error) {
	signer := v4.NewSigner(rgw.svc.Config.Credentials)

	endpoint := strings.TrimRight(rgw.svc.Endpoint, "/")
	url := fmt.Sprintf("%s/%s/%s?append&position=%d", endpoint, bucketName, objKey, position)
	req, err := http.NewRequest("PUT", url, body)
	if err != nil {
		return nil, err
	}

	if bodyLen > 0 {
		req.Header.Set("Content-Length", strconv.Itoa(bodyLen))
	}

	_, err = signer.Sign(req, body, "s3", "region", time.Now())
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)

	return resp, err
}
