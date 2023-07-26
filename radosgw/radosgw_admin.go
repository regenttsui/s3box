package radosgw

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/regenttsui/s3box/utils"
)

type UserInfo struct {
	Tenant               string       `json:"tenant"`
	UserID               string       `json:"user_id"`
	DisplayName          string       `json:"display_name"`
	Email                string       `json:"email"`
	Suspended            int64        `json:"suspended"`
	MaxBuckets           int64        `json:"max_buckets"`
	Subusers             []Subuser    `json:"subusers"`
	Keys                 []KeyClass   `json:"keys"`
	SwiftKeys            []KeyClass   `json:"swift_keys"`
	Caps                 []Capability `json:"caps"`
	OpMask               string       `json:"op_mask"`
	System               string       `json:"system"`
	Admin                string       `json:"admin"`
	AccessStorageClasses int64        `json:"access_storage_classes"`
	BucketQuota          Quota        `json:"bucket_quota"`
	UserQuota            Quota        `json:"user_quota"`
	// TempURLKeys          []interface{} `json:"temp_url_keys"`
	Type string `json:"type"`
	// MfaIDS               []interface{} `json:"mfa_ids"`
	Stats Stats `json:"stats"`
}

type Subuser struct {
	ID          string `json:"id"`
	Permissions string `json:"permissions"`
}

type Capability struct {
	Perm string `json:"perm"`
	Type string `json:"type"`
}

type Quota struct {
	Enabled    bool  `json:"enabled"`
	CheckOnRaw bool  `json:"check_on_raw"`
	MaxSize    int64 `json:"max_size"`
	MaxSizeKB  int64 `json:"max_size_kb"`
	MaxObjects int64 `json:"max_objects"`
}

type KeyClass struct {
	User      string `json:"user"`
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
}

type Stats struct {
	Size           int64 `json:"size"`
	SizeActual     int64 `json:"size_actual"`
	SizeUtilized   int64 `json:"size_utilized"`
	SizeKB         int64 `json:"size_kb"`
	SizeKBActual   int64 `json:"size_kb_actual"`
	SizeKBUtilized int64 `json:"size_kb_utilized"`
	NumObjects     int64 `json:"num_objects"`
}

type Quotas struct {
	BucketQuota Quota `json:"bucket_quota"`
	UserQuota   Quota `json:"user_quota"`
}

type BucketInfo []BucketInfoElement

type BucketInfoElement struct {
	Bucket        string `json:"bucket"`
	NumShards     int64  `json:"num_shards"`
	Tenant        string `json:"tenant"`
	Zonegroup     string `json:"zonegroup"`
	PlacementRule string `json:"placement_rule"`
	ID            string `json:"id"`
	Marker        string `json:"marker"`
	Owner         string `json:"owner"`
	Ver           string `json:"ver"`
	MasterVer     string `json:"master_ver"`
	Mtime         string `json:"mtime"`
	MaxMarker     string `json:"max_marker"`
	Usage         Usage  `json:"usage"`
	BucketQuota   Quota  `json:"bucket_quota"`
}

type Usage struct {
	RgwMain RgwMain `json:"rgw.main"`
}

type RgwMain struct {
	Size           int64 `json:"size"`
	SizeActual     int64 `json:"size_actual"`
	SizeUtilized   int64 `json:"size_utilized"`
	SizeKB         int64 `json:"size_kb"`
	SizeKBActual   int64 `json:"size_kb_actual"`
	SizeKBUtilized int64 `json:"size_kb_utilized"`
	NumObjects     int64 `json:"num_objects"`
}

func (rgw *RgwClient) PutUserQuota(uid string, body io.ReadSeeker) (*http.Response, error) {
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

func (rgw *RgwClient) PutUserBucketQuota(uid string, body io.ReadSeeker) (*http.Response, error) {
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

func (rgw *RgwClient) PutBucketQuota(uid, bucketName string, body io.ReadSeeker) (*http.Response, error) {
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

func (rgw *RgwClient) GetUserQuota(uid string) (*Quota, error) {
	url := fmt.Sprintf("%s/admin/user?quota&uid=%s&quota-type=user", *rgw.config.Endpoint, uid)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := rgw.buildSignerV2AndSendReq(req)
	if err != nil {
		return nil, err
	}

	buff, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var quota Quota
	err = json.Unmarshal(buff, &quota)
	if err != nil {
		return nil, err
	}

	return &quota, err
}

func (rgw *RgwClient) GetUserBucketQuota(uid string) (*Quota, error) {
	url := fmt.Sprintf("%s/admin/user?quota&uid=%s&quota-type=bucket", *rgw.config.Endpoint, uid)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := rgw.buildSignerV2AndSendReq(req)
	if err != nil {
		return nil, err
	}

	buff, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var quota Quota
	err = json.Unmarshal(buff, &quota)
	if err != nil {
		return nil, err
	}

	return &quota, err
}

// GetBucketInfo aka GetBucketQuota/GetBucketStats
func (rgw *RgwClient) GetBucketInfo(uid, bucketName string) (*BucketInfo, error) {
	url := fmt.Sprintf("%s/admin/bucket?uid=%s&bucket=%s&stats=True", *rgw.config.Endpoint, uid, bucketName)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := rgw.buildSignerV2AndSendReq(req)
	if err != nil {
		return nil, err
	}

	buff, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var bucketInfo BucketInfo
	err = json.Unmarshal(buff, &bucketInfo)
	if err != nil {
		return nil, err
	}

	return &bucketInfo, err
}
