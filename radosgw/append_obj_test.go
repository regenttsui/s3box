package radosgw

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	. "github.com/smartystreets/goconvey/convey"
	"net/http"
	"os"
	"testing"
	"time"
)

func buildRGWClient(t *testing.T) *RGWClient {
	t.Helper()
	conf := &aws.Config{
		Endpoint:         aws.String("http://endpoint/"),
		Region:           aws.String("mock-region"),
		S3ForcePathStyle: aws.Bool(true),
		DisableSSL:       aws.Bool(true),
		Credentials:      credentials.NewStaticCredentials("ak", "sk", ""),
		LogLevel:         aws.LogLevel(aws.LogDebugWithSigning),
		Logger:           aws.NewDefaultLogger(),
	}
	httpClient := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        50,
			MaxConnsPerHost:     20,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     5 * time.Minute,
		},
	}
	rgwClient := NewRGWClient(conf, httpClient)
	return rgwClient
}

func TestRGWClient_AppendObjV2(t *testing.T) {
	rgw := buildRGWClient(t)

	Convey("TestRGWClient_AppendObjV2", t, func() {
		type args struct {
			bucketName string
			objKey     string
			position   uint64
			fileName   string
		}
		tests := []struct {
			name    string
			args    args
			want    int
			wantErr bool
		}{
			{"AppendObjV2 should success", args{"bkt", "obj", 0, "D:/test.txt"}, 200, false},
		}

		for _, tt := range tests {
			file, err := os.Open(tt.args.fileName)
			if err != nil {
				t.Logf("Couldn't open file %v to upload. Here's why: %v\n", tt.args.fileName, err)
			}
			defer file.Close()
			Convey(tt.name, func() {
				got, err := rgw.AppendObjV2(tt.args.bucketName, tt.args.objKey, tt.args.position, file)
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
			fileName   string
		}
		tests := []struct {
			name    string
			args    args
			want    int
			wantErr bool
		}{
			{"AppendObjV4 with not zero position should success", args{"bkt", "obj", 8, "D:/test.txt"}, 200, false},
		}

		for _, tt := range tests {
			file, err := os.Open(tt.args.fileName)
			if err != nil {
				t.Logf("Couldn't open file %v to upload. Here's why: %v\n", tt.args.fileName, err)
			}
			defer file.Close()
			Convey(tt.name, func() {
				got, err := rgw.AppendObjV4(tt.args.bucketName, tt.args.objKey, tt.args.position, file)
				So(got, ShouldNotBeNil)
				So(got.StatusCode, ShouldEqual, tt.want)
				So(err, ShouldBeNil)
				t.Log(got)
			})
		}
	})
}
