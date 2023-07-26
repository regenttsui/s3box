package radosgw

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
)

func TestRgwClient_PutUserQuota(t *testing.T) {
	quota := Quota{
		Enabled:    false,
		CheckOnRaw: false,
		MaxSize:    -1,
		MaxSizeKB:  0,
		MaxObjects: -1,
	}
	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(quota)
	if err != nil {
		return
	}

	response, err := rgw.PutUserQuota("quota-user", bytes.NewReader(buf.Bytes()))
	if err != nil {
		return
	}
	fmt.Println(response)
}

func TestRgwClient_PutUserBucketQuota(t *testing.T) {
	quota := Quota{
		Enabled:    true,
		CheckOnRaw: false,
		MaxSizeKB:  100,
		MaxObjects: 100,
	}
	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(quota)
	if err != nil {
		return
	}

	response, err := rgw.PutUserBucketQuota("quota-user", bytes.NewReader(buf.Bytes()))
	if err != nil {
		return
	}
	fmt.Println(response)
}

func TestRgwClient_PutBucketQuota(t *testing.T) {
	quota := Quota{
		Enabled:    false,
		CheckOnRaw: false,
		MaxSizeKB:  200,
		MaxObjects: 100,
	}
	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(quota)
	if err != nil {
		return
	}

	response, err := rgw.PutBucketQuota("quota-user", "test-bkt", bytes.NewReader(buf.Bytes()))
	if err != nil {
		return
	}
	fmt.Println(response)
}

func TestRgwClient_GetUserQuota(t *testing.T) {
	quota, err := rgw.GetUserQuota("quota-user")
	if err != nil {
		return
	}
	fmt.Println(quota)
}

func TestRgwClient_GetUserBucketQuota(t *testing.T) {
	quota, err := rgw.GetUserBucketQuota("quota-user")
	if err != nil {
		return
	}
	fmt.Println(quota)
}

func TestRgwClient_GetBucketInfo(t *testing.T) {
	bucketInfo, err := rgw.GetBucketInfo("quota-user", "test")
	if err != nil {
		return
	}
	fmt.Println(bucketInfo)
}
