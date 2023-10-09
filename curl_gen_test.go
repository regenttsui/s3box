package s3box

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestCurlGen(t *testing.T) {
	conf := &aws.Config{
		Endpoint:         aws.String("http://endpoint"),
		Region:           aws.String("mock-region"),
		S3ForcePathStyle: aws.Bool(true),
		DisableSSL:       aws.Bool(true),
		Credentials:      credentials.NewStaticCredentials("ak", "sk", ""),
		LogLevel:         aws.LogLevel(aws.LogDebugWithSigning),
		Logger:           aws.NewDefaultLogger(),
	}
	Convey("TestCurlGen", t, func() {
		type args struct {
			method   string
			url      string
			body     string
			md5      string
			bodyType BodyType
		}
		tests := []struct {
			name string
			args args
		}{
			{"CurlGen with body should success", args{"PUT", "http://endpoint/test/?tagging",
				"<?xml version=\"1.0\" encoding=\"UTF-8\"?><Tagging xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><TagSet><Tag><Key>Test 123</Key><Value>Test 123</Value></Tag></TagSet></Tagging>",
				"DB1C1AB13CBD57A171AA7425C4481650", XMLBody}},
			{"CurlGen without body should success", args{"DELETE", "http://endpoint/test/?tagging",
				"", "", NoBody}},
		}
		for _, tt := range tests {
			Convey(tt.name, func() {
				got := CurlGen(conf, tt.args.method, tt.args.url, tt.args.body, tt.args.md5, tt.args.bodyType)
				So(got, ShouldNotEqual, "curl -i -v -X ")
				t.Log(got)
			})
		}
	})
}
