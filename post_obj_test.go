package s3box

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestBoxClient_PostObject(t *testing.T) {
	svc := buildS3Client(t)
	bc := NewBoxClient(svc)

	Convey("TestBoxClient_PostObject", t, func() {
		type args struct {
			bucket         string
			objKey         string
			expirationTime string
			filePath       string
		}
		tests := []struct {
			name    string
			args    args
			want    int
			wantErr bool
		}{
			{"RemoveCaps should success",
				args{"bkt", "abc", "2023-08-04T00:00:00Z", "./abc.txt"},
				204,
				false,
			},
		}

		for _, tt := range tests {
			Convey(tt.name, func() {
				got, err := bc.PostObject(tt.args.bucket, tt.args.objKey, tt.args.expirationTime, tt.args.filePath)
				So(got, ShouldNotBeNil)
				So(got.StatusCode, ShouldEqual, tt.want)
				So(err, ShouldBeNil)
				t.Log(got)
			})
		}
	})
}
