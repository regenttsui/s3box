package s3box

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func buildS3Client(t *testing.T) *s3.S3 {
	t.Helper()
	conf := &aws.Config{
		Endpoint:         aws.String("http://endpoint/"),
		Region:           aws.String("mock-region"),
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
	svc := buildS3Client(t)
	bc := NewBucketCleaner(svc)

	Convey("TestBucketCleaner_DeleteAllBuckets", t, func() {
		type args struct {
			containedStr string
		}
		tests := []struct {
			name    string
			args    args
			want    int
			wantErr bool
		}{
			{"DeleteAllBuckets should success",
				args{"abc"},
				204,
				false,
			},
		}

		for _, tt := range tests {
			Convey(tt.name, func() {
				err := bc.DeleteAllBuckets(tt.args.containedStr)
				So(err, ShouldBeNil)
			})
		}
	})
}

func TestBucketCleaner_EmptyBucket(t *testing.T) {
	svc := buildS3Client(t)
	bc := NewBucketCleaner(svc)

	Convey("TestBucketCleaner_EmptyBucket", t, func() {
		type args struct {
			bucket          string
			deleteWorkerNum int
			objChanCap      int
			multiDel        bool
			deleteBucket    bool
		}
		tests := []struct {
			name    string
			args    args
			want    int
			wantErr bool
		}{
			{"EmptyBucket should success",
				args{"abc", 5, 1000, true, false},
				204,
				false,
			},
		}

		for _, tt := range tests {
			Convey(tt.name, func() {
				err := bc.EmptyBucket(tt.args.bucket, tt.args.deleteWorkerNum, tt.args.objChanCap, tt.args.multiDel, tt.args.deleteBucket)
				So(err, ShouldBeNil)
			})
		}
	})
}
