package radosgw

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	. "github.com/smartystreets/goconvey/convey"
	"io"
	"testing"
)

func buildRGWClient(t *testing.T) *RGWClient {
	t.Helper()
	conf := &aws.Config{
		Endpoint:         aws.String("http://endpoint/"),
		Region:           aws.String("mock-region"),
		S3ForcePathStyle: aws.Bool(true),
		DisableSSL:       aws.Bool(true),
		Credentials:      credentials.NewStaticCredentials("ak", "sk", ""),
		LogLevel:         aws.LogLevel(aws.LogDebug),
	}
	rgwClient := NewRGWClient(conf)
	return rgwClient
}

func TestRGWClient_AppendObjV2(t *testing.T) {
	rgw := buildRGWClient(t)

	Convey("TestRGWClient_AppendObjV2", t, func() {
		type args struct {
			bucketName string
			objKey     string
			position   uint64
			body       io.ReadSeeker
		}
		tests := []struct {
			name    string
			args    args
			want    int
			wantErr bool
		}{
			{"AppendObjV2 should success", args{"bkt", "obj", 0, nil}, 200, false},
		}

		for _, tt := range tests {
			Convey(tt.name, func() {
				got, err := rgw.AppendObjV2(tt.args.bucketName, tt.args.objKey, tt.args.position, tt.args.body)
				So(got, ShouldNotBeNil)
				So(got.StatusCode, ShouldEqual, tt.want)
				So(err, ShouldBeNil)
				t.Log(got)
			})
		}
	})
}

func TestRGWClient_AppendObjV4(t *testing.T) {
	rgw := buildRGWClient(t)

	Convey("TestRGWClient_AppendObjV4", t, func() {
		type args struct {
			bucketName string
			objKey     string
			position   uint64
			body       io.ReadSeeker
		}
		tests := []struct {
			name    string
			args    args
			want    int
			wantErr bool
		}{
			{"AppendObjV4 should success", args{"bkt", "obj", 0, nil}, 200, false},
		}

		for _, tt := range tests {
			Convey(tt.name, func() {
				got, err := rgw.AppendObjV4(tt.args.bucketName, tt.args.objKey, tt.args.position, tt.args.body)
				So(got, ShouldNotBeNil)
				So(got.StatusCode, ShouldEqual, tt.want)
				So(err, ShouldBeNil)
				t.Log(got)
			})
		}
	})
}
