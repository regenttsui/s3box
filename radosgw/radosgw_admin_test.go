package radosgw

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
)

func TestRGWClient_PutUserQuota(t *testing.T) {
	rgw := buildRGWClient(t)
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

func TestRGWClient_PutUserBucketQuota(t *testing.T) {
	rgw := buildRGWClient(t)
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

func TestRGWClient_PutBucketQuota(t *testing.T) {
	rgw := buildRGWClient(t)
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

func TestRGWClient_GetUserQuota(t *testing.T) {
	rgw := buildRGWClient(t)
	quota, err := rgw.GetUserQuota("quota-user")
	if err != nil {
		return
	}
	fmt.Println(quota)
}

func TestRGWClient_GetUserBucketQuota(t *testing.T) {
	rgw := buildRGWClient(t)
	quota, err := rgw.GetUserBucketQuota("quota-user")
	if err != nil {
		return
	}
	fmt.Println(quota)
}

func TestRGWClient_GetBucketInfo(t *testing.T) {
	rgw := buildRGWClient(t)
	bucketInfo, err := rgw.GetBucketInfo("quota-user", "test")
	if err != nil {
		return
	}
	fmt.Println(bucketInfo)
}

func TestRGWClient_GetUserInfo(t *testing.T) {
	rgw := buildRGWClient(t)
	got, err := rgw.GetUserInfo("testid", "True")
	if err != nil {
		t.Errorf("RGWClient.GetUserInfo() error = %v", err)
		return
	}
	t.Log(got)
}

func TestRGWClient_CreateUser(t *testing.T) {
	rgw := buildRGWClient(t)
	userConf := UserConf{
		Uid:         "new-user",
		DisplayName: "dis-new-user",
		// Email:       "",
		// KeyType:     "",
		// AccessKey:   "",
		// SecretKey:   "",
		// UserCaps:    "",
		// GenerateKey: false,
		// Suspended:   0,
		MaxBuckets: 100,
		// Tenant:      "",
		// System:      false,
		// OpMask:      "read, write, delete",
	}
	got, err := rgw.CreateUser(&userConf)
	if err != nil {
		t.Errorf("RGWClient.CreateUser() error = %v", err)
		return
	}
	t.Log(got)
}

func TestRGWClient_ModifyUser(t *testing.T) {
	rgw := buildRGWClient(t)
	userConf := UserConf{
		Uid:         "new-user",
		DisplayName: "dis-new-user",
		Email:       "abc@email.com",
		// KeyType:     "",
		// AccessKey:   "",
		// SecretKey:   "",
		// UserCaps:    "",
		// GenerateKey: false,
		// Suspended:   0,
		// MaxBuckets: 100,
		// Tenant:      "",
		// System:      false,
		// OpMask:      "read, write, delete",
	}
	got, err := rgw.ModifyUser(&userConf)
	if err != nil {
		t.Errorf("RGWClient.ModifyUser() error = %v", err)
		return
	}
	t.Log(got)
}

func TestRGWClient_RemoveUser(t *testing.T) {
	rgw := buildRGWClient(t)
	got, err := rgw.RemoveUser("new-user")
	if err != nil {
		t.Errorf("RGWClient.RemoveUser() error = %v", err)
		return
	}
	t.Log(got)
}

func TestRGWClient_CreateKey(t *testing.T) {
	rgw := buildRGWClient(t)
	userConf := UserConf{
		Uid:       "new-user",
		KeyType:   "s3",
		AccessKey: "jifdjiwhfiojidwpoopweru",
		SecretKey: "nufewpowqfnuifgqpwjfcnmhur",
	}
	got, err := rgw.CreateKey(&userConf)
	if err != nil {
		t.Errorf("RGWClient.CreateKey() error = %v", err)
		return
	}
	t.Log(got)
}

func TestRGWClient_RemoveKey(t *testing.T) {
	rgw := buildRGWClient(t)
	userConf := UserConf{
		Uid:       "new-user",
		KeyType:   "s3",
		AccessKey: "jifdjiwhfiojidwpoopweru",
	}
	got, err := rgw.RemoveKey(&userConf)
	if err != nil {
		t.Errorf("RGWClient.RemoveKey() error = %v", err)
		return
	}
	t.Log(got)
}

func TestRGWClient_AddCaps(t *testing.T) {
	rgw := buildRGWClient(t)
	got, err := rgw.AddCaps("new-user", "users=write")
	if err != nil {
		t.Errorf("RGWClient.AddCaps() error = %v", err)
		return
	}
	t.Log(got)
}

func TestRGWClient_RemoveCaps(t *testing.T) {
	rgw := buildRGWClient(t)
	got, err := rgw.RemoveCaps("new-user", "users=write")
	if err != nil {
		t.Errorf("RGWClient.RemoveCaps() error = %v", err)
		return
	}
	t.Log(got)
}
