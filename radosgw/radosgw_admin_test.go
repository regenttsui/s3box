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

func TestRgwClient_GetUserInfo(t *testing.T) {
	got, err := rgw.GetUserInfo("testid", "True")
	if err != nil {
		t.Errorf("RgwClient.GetUserInfo() error = %v", err)
		return
	}
	t.Log(got)
}

func TestRgwClient_CreateUser(t *testing.T) {
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
		t.Errorf("RgwClient.CreateUser() error = %v", err)
		return
	}
	t.Log(got)
}

func TestRgwClient_ModifyUser(t *testing.T) {
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
		t.Errorf("RgwClient.ModifyUser() error = %v", err)
		return
	}
	t.Log(got)
}

func TestRgwClient_RemoveUser(t *testing.T) {
	got, err := rgw.RemoveUser("new-user")
	if err != nil {
		t.Errorf("RgwClient.RemoveUser() error = %v", err)
		return
	}
	t.Log(got)
}

func TestRgwClient_CreateKey(t *testing.T) {
	userConf := UserConf{
		Uid:       "new-user",
		KeyType:   "s3",
		AccessKey: "jifdjiwhfiojidwpoopweru",
		SecretKey: "nufewpowqfnuifgqpwjfcnmhur",
	}
	got, err := rgw.CreateKey(&userConf)
	if err != nil {
		t.Errorf("RgwClient.CreateKey() error = %v", err)
		return
	}
	t.Log(got)
}

func TestRgwClient_RemoveKey(t *testing.T) {
	userConf := UserConf{
		Uid:       "new-user",
		KeyType:   "s3",
		AccessKey: "jifdjiwhfiojidwpoopweru",
	}
	got, err := rgw.RemoveKey(&userConf)
	if err != nil {
		t.Errorf("RgwClient.RemoveKey() error = %v", err)
		return
	}
	t.Log(got)
}

func TestRgwClient_AddCaps(t *testing.T) {
	got, err := rgw.AddCaps("new-user", "users=write")
	if err != nil {
		t.Errorf("RgwClient.AddCaps() error = %v", err)
		return
	}
	t.Log(got)
}

func TestRgwClient_RemoveCaps(t *testing.T) {
	got, err := rgw.RemoveCaps("new-user", "users=write")
	if err != nil {
		t.Errorf("RgwClient.RemoveCaps() error = %v", err)
		return
	}
	t.Log(got)
}
