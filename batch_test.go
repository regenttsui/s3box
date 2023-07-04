package s3box

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"testing"
)

var (
	svc *s3.S3
)

func init() {
	svc = BuildClient()
}

func BuildClient() *s3.S3 {
	conf := &aws.Config{
		Endpoint:         aws.String("http://endpoint/"),
		Region:           aws.String("fake-region"),
		S3ForcePathStyle: aws.Bool(true),
		DisableSSL:       aws.Bool(true),
		Credentials:      credentials.NewStaticCredentials("ak", "sk", ""),
		//LogLevel:         aws.LogLevel(aws.LogDebug),
	}
	sess := session.Must(session.NewSessionWithOptions(session.Options{Config: *conf}))
	svc := s3.New(sess)
	return svc
}

func TestBucketCleaner_DeleteAllBuckets(t *testing.T) {
	bc := NewBucketCleaner(svc)
	err := bc.DeleteAllBuckets("abc")
	if err != nil {
		return
	}
}

func TestBucketCleaner_EmptyBucket(t *testing.T) {
	bc := NewBucketCleaner(svc)
	err := bc.EmptyBucket("abc", 5, 1000, true, false)
	if err != nil {
		return
	}
}
