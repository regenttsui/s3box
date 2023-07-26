package radosgw

import (
	"testing"

	"github.com/aws/aws-sdk-go/service/s3"
)

func TestRgwClient_ListTopics(t *testing.T) {
	got, err := rgw.ListTopics()
	if err != nil {
		t.Errorf("RgwClient.ListTopics() error = %v", err)
		return
	}
	t.Log(got)
}

func TestRgwClient_CreateTopic(t *testing.T) {
	got, err := rgw.CreateTopic("abc", "http://abc")
	if err != nil {
		t.Errorf("RgwClient.CreateTopic() error = %v", err)
		return
	}
	t.Log(got)
}

func TestRgwClient_GetTopic(t *testing.T) {
	got, err := rgw.GetTopic("arn:aws:sns:default::abc")
	if err != nil {
		t.Errorf("RgwClient.GetTopic() error = %v", err)
		return
	}
	t.Log(got)
}

func TestRgwClient_DeleteTopic(t *testing.T) {
	got, err := rgw.DeleteTopic("arn:aws:sns:default::abc")
	if err != nil {
		t.Errorf("RgwClient.DeleteTopic() error = %v", err)
		return
	}
	t.Log(got)
}

func TestRgwClient_CreateNotification(t *testing.T) {
	var (
		metaData MetaDataFilter
		events   = []string{
			s3.EventS3ObjectCreatedPost,
			s3.EventS3ObjectCreatedPut,
			s3.EventS3ObjectCreatedCompleteMultipartUpload,
			s3.EventS3ObjectCreatedCopy,
		}
	)
	got, err := rgw.CreateNotification("arn:aws:sns:default::abc", "test", "notifId", "pre", "suf", metaData, events)
	if err != nil {
		t.Errorf("RgwClient.CreateNotification() error = %v", err)
		return
	}
	t.Log(got)
}

func TestRgwClient_GetNotification(t *testing.T) {
	type args struct {
		bucket         string
		notificationId string
	}
	tests := []struct {
		name string
		args args
		want *NotificationConfiguration
	}{
		{
			name: "listNotification",
			args: args{
				bucket:         "test",
				notificationId: "",
			},
			want: &NotificationConfiguration{},
		},
		{
			name: "getNotification",
			args: args{
				bucket:         "test",
				notificationId: "notifId",
			},
			want: &NotificationConfiguration{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := rgw.GetNotification(tt.args.bucket, tt.args.notificationId)
			if err != nil {
				t.Errorf("RgwClient.GetNotification() error = %v", err)
				return
			}
			t.Log(got)
		})
	}
}

func TestRgwClient_DeleteNotification(t *testing.T) {
	got, err := rgw.DeleteNotification("test", "notifId")
	if err != nil {
		t.Errorf("RgwClient.DeleteNotification() error = %v", err)
		return
	}
	t.Log(got)
}
