package radosgw

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"testing"
)

var (
	rgw *RgwClient
)

func init() {
	rgw = BuildClient()
}

func BuildClient() *RgwClient {
	conf := &aws.Config{
		Endpoint:         aws.String("http://endpoint/"),
		Region:           aws.String("fake-region"),
		S3ForcePathStyle: aws.Bool(true),
		DisableSSL:       aws.Bool(true),
		Credentials:      credentials.NewStaticCredentials("ak", "sk", ""),
		LogLevel:         aws.LogLevel(aws.LogDebug),
	}
	sess := session.Must(session.NewSessionWithOptions(session.Options{Config: *conf}))
	svc := s3.New(sess)
	rgwClient := NewRgwClient(svc)
	return rgwClient
}

func TestRgwClient_AppendObjV2(t *testing.T) {
	response, err := rgw.AppendObjV2("bkt", "obj", 0, nil)
	if err != nil {
		return
	}
	fmt.Println(response)
}

func TestRgwClient_AppendObjV4(t *testing.T) {
	response, err := rgw.AppendObjV4("bkt", "obj", 0, nil)
	if err != nil {
		return
	}
	fmt.Println(response)
}
