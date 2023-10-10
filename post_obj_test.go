package s3box

import (
	"encoding/base64"
	"fmt"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

func TestBoxClient_PostObject(t *testing.T) {
	svc := buildS3Client(t)
	bc := NewBoxClient(svc)

	v, err := svc.Config.Credentials.Get()
	if err != nil {
		return
	}
	currentTime := time.Now()
	currentDate := currentTime.Format("20060102")
	xAmzDate := currentTime.Format("20060102T150405Z")
	credential := fmt.Sprintf("%s/%s/%s/s3/aws4_request", v.AccessKeyID, currentDate, *svc.Config.Region)
	algorithm := "AWS4-HMAC-SHA256"

	Convey("TestBoxClient_PostObject", t, func() {
		type args struct {
			bucket     string
			objKey     string
			filePath   string
			fields     map[string]string
			expiration string
		}
		tests := []struct {
			name    string
			args    args
			want    int
			wantErr bool
		}{
			{"PostObject should success",
				args{"test", "test-post.txt", "D:/test.txt", map[string]string{"x-amz-meta-test": "test-val"}, "2023-11-04T00:00:00Z"},
				204,
				false,
			},
		}

		for _, tt := range tests {
			Convey(tt.name, func() {
				tt.args.fields["key"] = tt.args.objKey
				tt.args.fields["x-amz-algorithm"] = algorithm
				tt.args.fields["x-amz-credential"] = credential
				tt.args.fields["x-amz-date"] = xAmzDate

				//fields in conditions and form need to correspond one by one except for bucket
				policy := fmt.Sprintf(`{"expiration": "%s", "conditions": [{"bucket": "%s"}`, tt.args.expiration, tt.args.bucket)
				for k, s := range tt.args.fields {
					policy = fmt.Sprintf(`%s, {"%s": "%s"}`, policy, k, s)
				}
				policy += "]}"
				encodedPolicy := base64.StdEncoding.EncodeToString([]byte(policy))
				tt.args.fields["policy"] = encodedPolicy

				got, err := bc.PostObject(tt.args.bucket, tt.args.objKey, currentDate, encodedPolicy, *svc.Config.Region, tt.args.filePath, tt.args.fields)
				So(got, ShouldNotBeNil)
				So(got.StatusCode, ShouldEqual, tt.want)
				So(err, ShouldBeNil)
				t.Log(got)
			})
		}
	})
}
