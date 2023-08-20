package radosgw

import (
	"github.com/aws/aws-sdk-go/service/s3"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestRGWClient_ListTopics(t *testing.T) {
	rgw := buildRGWClient(t)

	Convey("ListTopics should success", t, func() {
		got, err := rgw.ListTopics()
		So(got, ShouldNotBeNil)
		So(err, ShouldBeNil)
		t.Log(got)
	})
}

func TestRGWClient_CreateTopic(t *testing.T) {
	rgw := buildRGWClient(t)

	Convey("TestRGWClient_CreateTopic", t, func() {
		type args struct {
			topicName    string
			pushEndpoint string
		}
		tests := []struct {
			name    string
			args    args
			want    string
			wantErr bool
		}{
			{"CreateTopic should success", args{"abc", "http://abc"}, "arn:aws:sns:default::abc", false},
		}

		for _, tt := range tests {
			Convey(tt.name, func() {
				got, err := rgw.CreateTopic(tt.args.topicName, tt.args.pushEndpoint)
				So(got, ShouldEqual, tt.want)
				So(err, ShouldBeNil)
				t.Log(got)
			})
		}
	})
}

func TestRGWClient_GetTopic(t *testing.T) {
	rgw := buildRGWClient(t)

	Convey("TestRGWClient_GetTopic", t, func() {
		type args struct {
			topicArn string
		}
		tests := []struct {
			name    string
			args    args
			want    string
			wantErr bool
		}{
			{"GetTopic should success", args{"arn:aws:sns:default::abc"}, "arn:aws:sns:default::abc", false},
		}

		for _, tt := range tests {
			Convey(tt.name, func() {
				got, err := rgw.GetTopic(tt.args.topicArn)
				So(got, ShouldNotBeNil)
				So(got.GetTopicResult.Topic.TopicArn, ShouldEqual, tt.want)
				So(err, ShouldBeNil)
				t.Log(got)
			})
		}
	})
}

func TestRGWClient_DeleteTopic(t *testing.T) {
	rgw := buildRGWClient(t)

	Convey("TestRGWClient_DeleteTopic", t, func() {
		type args struct {
			topicArn string
		}
		tests := []struct {
			name    string
			args    args
			want    int
			wantErr bool
		}{
			{"DeleteTopic should success", args{"arn:aws:sns:default::abc"}, 204, false},
		}

		for _, tt := range tests {
			Convey(tt.name, func() {
				got, err := rgw.DeleteTopic(tt.args.topicArn)
				So(got, ShouldNotBeNil)
				So(got, ShouldEqual, tt.want)
				So(err, ShouldBeNil)
				t.Log(got)
			})
		}
	})
}

func TestRGWClient_CreateNotification(t *testing.T) {
	rgw := buildRGWClient(t)

	Convey("TestRGWClient_CreateNotification", t, func() {
		type args struct {
			topicArn       string
			bucket         string
			notificationId string
			prefix         string
			suffix         string
			metaData       MetaDataFilter
			events         []string
		}
		tests := []struct {
			name    string
			args    args
			want    int
			wantErr bool
		}{
			{"CreateNotification should success",
				args{"arn:aws:sns:default::abc", "test", "notifId", "pre", "suf", MetaDataFilter{}, []string{
					s3.EventS3ObjectCreatedPost,
					s3.EventS3ObjectCreatedPut,
					s3.EventS3ObjectCreatedCompleteMultipartUpload,
					s3.EventS3ObjectCreatedCopy,
				}}, 200, false},
		}

		for _, tt := range tests {
			Convey(tt.name, func() {
				got, err := rgw.CreateNotification(tt.args.topicArn, tt.args.bucket, tt.args.notificationId,
					tt.args.prefix, tt.args.suffix, tt.args.metaData, tt.args.events)
				So(got, ShouldNotBeNil)
				So(got, ShouldEqual, tt.want)
				So(err, ShouldBeNil)
				t.Log(got)
			})
		}
	})
}

func TestRGWClient_GetNotification(t *testing.T) {
	rgw := buildRGWClient(t)

	Convey("TestRGWClient_GetNotification", t, func() {
		type args struct {
			bucket         string
			notificationId string
		}
		tests := []struct {
			name   string
			args   args
			want   int
			assert Assertion
		}{
			{"listNotification should success", args{"test", ""}, 1, ShouldEqual},
			{"getNotification should success", args{"test", "notifId"}, 1, ShouldBeGreaterThanOrEqualTo},
		}

		for _, tt := range tests {
			Convey(tt.name, func() {
				got, err := rgw.GetNotification(tt.args.bucket, tt.args.notificationId)
				So(got, ShouldNotBeNil)
				So(len(got.TopicConfiguration), tt.assert, tt.want)
				So(err, ShouldBeNil)
				t.Log(got)
			})
		}
	})
}

func TestRGWClient_DeleteNotification(t *testing.T) {
	rgw := buildRGWClient(t)

	Convey("TestRGWClient_DeleteNotification", t, func() {
		type args struct {
			bucket         string
			notificationId string
		}
		tests := []struct {
			name    string
			args    args
			want    int
			wantErr bool
		}{
			{"DeleteNotification should success", args{"test", "notifId"}, 204, false},
		}

		for _, tt := range tests {
			Convey(tt.name, func() {
				got, err := rgw.DeleteNotification(tt.args.bucket, tt.args.notificationId)
				So(got, ShouldNotBeNil)
				So(got, ShouldEqual, tt.want)
				So(err, ShouldBeNil)
				t.Log(got)
			})
		}
	})
}
