package radosgw

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/google/go-querystring/query"
	"github.com/regenttsui/s3box/utils"
)

type UserInfo struct {
	Tenant      string       `json:"tenant"`
	UserID      string       `json:"user_id"`
	DisplayName string       `json:"display_name"`
	Email       string       `json:"email"`
	Suspended   int64        `json:"suspended"`
	MaxBuckets  int64        `json:"max_buckets"`
	Subusers    []Subuser    `json:"subusers"`
	Keys        []KeyClass   `json:"keys"`
	SwiftKeys   []KeyClass   `json:"swift_keys"`
	Caps        []Capability `json:"caps"`
	OpMask      string       `json:"op_mask"`
	System      string       `json:"system"`
	Admin       string       `json:"admin"`
	BucketQuota Quota        `json:"bucket_quota"`
	UserQuota   Quota        `json:"user_quota"`
	TempURLKeys []string     `json:"temp_url_keys"`
	Type        string       `json:"type"`
	MfaIDS      []string     `json:"mfa_ids"`
	Stats       Stats        `json:"stats"`
}

type UserConf struct {
	Uid         string `url:"uid,omitempty"`
	DisplayName string `url:"display-name,omitempty"`
	Email       string `url:"email,omitempty"`
	KeyType     string `url:"key-type,omitempty"`
	AccessKey   string `url:"access-key,omitempty"`
	SecretKey   string `url:"secret-key,omitempty"`
	UserCaps    string `url:"user-caps,omitempty"`
	GenerateKey bool   `url:"generate-key,omitempty"`
	Suspended   int64  `url:"suspended,omitempty"`
	MaxBuckets  int64  `url:"max-buckets,omitempty"`
	Tenant      string `url:"tenant,omitempty"`
	System      bool   `url:"system,omitempty"`
	OpMask      string `url:"op-mask,omitempty"`
}

type Subuser struct {
	ID          string `json:"id"`
	Permissions string `json:"permissions"`
}

type Capability struct {
	Type string `json:"type"`
	Perm string `json:"perm"`
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
	RGWMain RGWMain `json:"rgw.main"`
}

type RGWMain struct {
	Size           int64 `json:"size"`
	SizeActual     int64 `json:"size_actual"`
	SizeUtilized   int64 `json:"size_utilized"`
	SizeKB         int64 `json:"size_kb"`
	SizeKBActual   int64 `json:"size_kb_actual"`
	SizeKBUtilized int64 `json:"size_kb_utilized"`
	NumObjects     int64 `json:"num_objects"`
}

func (rgw *RGWClient) PutUserQuota(uid string, body io.ReadSeeker) (*http.Response, error) {
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

func (rgw *RGWClient) PutUserBucketQuota(uid string, body io.ReadSeeker) (*http.Response, error) {
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

func (rgw *RGWClient) PutBucketQuota(uid, bucketName string, body io.ReadSeeker) (*http.Response, error) {
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

func (rgw *RGWClient) GetUserQuota(uid string) (*Quota, error) {
	url := fmt.Sprintf("%s/admin/user?quota&uid=%s&quota-type=user", *rgw.config.Endpoint, uid)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := rgw.buildSignerV2AndSendReq(req)
	if resp != nil {
		defer resp.Body.Close()
	}
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

func (rgw *RGWClient) GetUserBucketQuota(uid string) (*Quota, error) {
	url := fmt.Sprintf("%s/admin/user?quota&uid=%s&quota-type=bucket", *rgw.config.Endpoint, uid)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := rgw.buildSignerV2AndSendReq(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return nil, err
	}

	//TODO handle not 200 response
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
func (rgw *RGWClient) GetBucketInfo(uid, bucketName string) (*BucketInfo, error) {
	url := fmt.Sprintf("%s/admin/bucket?uid=%s&bucket=%s&stats=True", *rgw.config.Endpoint, uid, bucketName)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := rgw.buildSignerV2AndSendReq(req)
	if resp != nil {
		defer resp.Body.Close()
	}
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

// GetUserInfo stats should be "True" or "False"
func (rgw *RGWClient) GetUserInfo(uid, stats string) (*UserInfo, error) {
	url := fmt.Sprintf("%s/admin/user?uid=%s&stats=%s", *rgw.config.Endpoint, uid, stats)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := rgw.buildSignerV2AndSendReq(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return nil, err
	}

	buff, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var userInfo UserInfo
	err = json.Unmarshal(buff, &userInfo)
	if err != nil {
		return nil, err
	}

	return &userInfo, err
}

func (rgw *RGWClient) CreateUser(userConf *UserConf) (*UserInfo, error) {
	if userConf.Uid == "" {
		return nil, errors.New("uid is required")
	}
	if userConf.DisplayName == "" {
		return nil, errors.New("displayName is required")
	}

	v, _ := query.Values(userConf)
	url := fmt.Sprintf("%s/admin/user?%s", *rgw.config.Endpoint, v.Encode())
	req, err := http.NewRequest("PUT", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := rgw.buildSignerV2AndSendReq(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return nil, err
	}

	buff, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var userInfo UserInfo
	err = json.Unmarshal(buff, &userInfo)
	if err != nil {
		return nil, err
	}

	return &userInfo, err
}

func (rgw *RGWClient) ModifyUser(userConf *UserConf) (*UserInfo, error) {
	if userConf.Uid == "" {
		return nil, errors.New("uid is required")
	}

	v, _ := query.Values(userConf)
	url := fmt.Sprintf("%s/admin/user?%s", *rgw.config.Endpoint, v.Encode())
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := rgw.buildSignerV2AndSendReq(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return nil, err
	}

	buff, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var userInfo UserInfo
	err = json.Unmarshal(buff, &userInfo)
	if err != nil {
		return nil, err
	}

	return &userInfo, err
}

func (rgw *RGWClient) RemoveUser(uid string) (*http.Response, error) {
	if uid == "" {
		return nil, errors.New("uid is required")
	}

	url := fmt.Sprintf("%s/admin/user?uid=%s", *rgw.config.Endpoint, uid)
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := rgw.buildSignerV2AndSendReq(req)

	return resp, err
}

func (rgw *RGWClient) CreateKey(userConf *UserConf) (*[]KeyClass, error) {
	if userConf.Uid == "" {
		return nil, errors.New("uid is required")
	}

	v, _ := query.Values(userConf)
	url := fmt.Sprintf("%s/admin/user?key&%s", *rgw.config.Endpoint, v.Encode())
	req, err := http.NewRequest("PUT", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := rgw.buildSignerV2AndSendReq(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return nil, err
	}

	buff, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var key []KeyClass
	err = json.Unmarshal(buff, &key)
	if err != nil {
		return nil, err
	}

	return &key, err
}

func (rgw *RGWClient) RemoveKey(userConf *UserConf) (*http.Response, error) {
	if userConf.AccessKey == "" {
		return nil, errors.New("access-key is required")
	}

	v, _ := query.Values(userConf)
	url := fmt.Sprintf("%s/admin/user?key&%s", *rgw.config.Endpoint, v.Encode())
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := rgw.buildSignerV2AndSendReq(req)

	return resp, err
}

func (rgw *RGWClient) AddCaps(uid, caps string) (*[]Capability, error) {
	if uid == "" {
		return nil, errors.New("uid is required")
	}

	url := fmt.Sprintf("%s/admin/user?caps&uid=%s&user-caps=%s", *rgw.config.Endpoint, uid, caps)
	req, err := http.NewRequest("PUT", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := rgw.buildSignerV2AndSendReq(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return nil, err
	}

	buff, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var allCaps []Capability
	err = json.Unmarshal(buff, &allCaps)
	if err != nil {
		return nil, err
	}

	return &allCaps, err
}

func (rgw *RGWClient) RemoveCaps(uid, caps string) (*[]Capability, error) {
	if uid == "" {
		return nil, errors.New("uid is required")
	}

	url := fmt.Sprintf("%s/admin/user?caps&uid=%s&user-caps=%s", *rgw.config.Endpoint, uid, caps)
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := rgw.buildSignerV2AndSendReq(req)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return nil, err
	}

	buff, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var allCaps []Capability
	err = json.Unmarshal(buff, &allCaps)
	if err != nil {
		return nil, err
	}

	return &allCaps, err
}
