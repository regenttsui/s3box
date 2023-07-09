package radosgw

import (
	"fmt"
	"github.com/regenttsui/s3box/utils"
	"net/http"
	"strings"
)

func (rgw *RgwClient) PutUserQuota(uid string, body *strings.Reader) (*http.Response, error) {
	url := fmt.Sprintf("%s/admin/user?quota&uid=%s&quota-type=user", *rgw.config.Endpoint, uid)
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

func (rgw *RgwClient) PutUserBucketQuota(uid string, body *strings.Reader) (*http.Response, error) {
	url := fmt.Sprintf("%s/admin/user?quota&uid=%s&quota-type=bucket", *rgw.config.Endpoint, uid)
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

func (rgw *RgwClient) PutBucketQuota(uid, bucketName string, body *strings.Reader) (*http.Response, error) {
	url := fmt.Sprintf("%s/admin/bucket?quota&uid=%s&bucket=%s", *rgw.config.Endpoint, uid, bucketName)
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

// GetUserQuota TODO define a struct to hold response for easier use by the client
func (rgw *RgwClient) GetUserQuota(uid string) (*http.Response, error) {
	url := fmt.Sprintf("%s/admin/user?quota&uid=%s&quota-type=user", *rgw.config.Endpoint, uid)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := rgw.buildSignerV2AndSendReq(req)

	return resp, err
}

func (rgw *RgwClient) GetUserBucketQuota(uid string) (*http.Response, error) {
	url := fmt.Sprintf("%s/admin/user?quota&uid=%s&quota-type=bucket", *rgw.config.Endpoint, uid)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := rgw.buildSignerV2AndSendReq(req)

	return resp, err
}

// GetBucketInfo aka GetBucketQuota/GetBucketStats
func (rgw *RgwClient) GetBucketInfo(uid, bucketName string) (*http.Response, error) {
	url := fmt.Sprintf("%s/admin/bucket?uid=%s&bucket=%s&stats=True", *rgw.config.Endpoint, uid, bucketName)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := rgw.buildSignerV2AndSendReq(req)

	return resp, err
}
