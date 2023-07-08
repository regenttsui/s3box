package radosgw

import (
	"fmt"
	"strings"
	"testing"
)

func TestRgwClient_PutUserQuota(t *testing.T) {
	body := strings.NewReader("{\"enabled\":false,\"check_on_raw\":false,\"max_size\":-1,\"max_size_kb\":0,\"max_objects\":-1}")
	response, err := rgw.PutUserQuota("test_quota", body)
	if err != nil {
		return
	}
	fmt.Println(response)
}

func TestRgwClient_PutUserBucketQuota(t *testing.T) {
	body := strings.NewReader("{\"enabled\":true,\"check_on_raw\":false,\"max_size_kb\":100,\"max_objects\":100}")
	response, err := rgw.PutUserBucketQuota("test_quota", body)
	if err != nil {
		return
	}
	fmt.Println(response)
}

func TestRgwClient_PutBucketQuota(t *testing.T) {
	body := strings.NewReader("{\"enabled\":false,\"check_on_raw\":false,\"max_size_kb\":200,\"max_objects\":100}")
	response, err := rgw.PutBucketQuota("test_quota", "test-bkt", body)
	if err != nil {
		return
	}
	fmt.Println(response)
}

func TestRgwClient_GetUserQuota(t *testing.T) {
	response, err := rgw.GetUserQuota("test_quota")
	if err != nil {
		return
	}
	fmt.Println(response)
}

func TestRgwClient_GetUserBucketQuota(t *testing.T) {
	response, err := rgw.GetUserBucketQuota("test_quota")
	if err != nil {
		return
	}
	fmt.Println(response)
}

func TestRgwClient_GetBucketInfo(t *testing.T) {
	response, err := rgw.GetBucketInfo("test_quota", "test-bkt")
	if err != nil {
		return
	}
	fmt.Println(response)
}
