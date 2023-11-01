package radosgw

import (
	"bytes"
	"encoding/json"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestRGWClient_PutUserQuota(t *testing.T) {
	rgw := buildRGWClient(t)

	Convey("TestRGWClient_PutUserQuota", t, func() {
		type args struct {
			uid   string
			quota Quota
		}
		tests := []struct {
			name    string
			args    args
			want    int
			wantErr bool
		}{
			{"PutUserQuota should success", args{"quota-user", Quota{
				Enabled:    false,
				CheckOnRaw: false,
				MaxSize:    -1,
				MaxSizeKB:  0,
				MaxObjects: -1,
			}}, 200, false},
		}

		for _, tt := range tests {
			Convey(tt.name, func() {
				var buf bytes.Buffer
				err := json.NewEncoder(&buf).Encode(tt.args.quota)
				So(err, ShouldBeNil)

				got, err := rgw.PutUserQuota(tt.args.uid, bytes.NewReader(buf.Bytes()))
				So(got, ShouldNotBeNil)
				So(got.StatusCode, ShouldEqual, tt.want)
				So(err, ShouldBeNil)
				t.Log(got)
			})
		}
	})
}

func TestRGWClient_PutUserBucketQuota(t *testing.T) {
	rgw := buildRGWClient(t)

	Convey("TestRGWClient_PutUserBucketQuota", t, func() {
		type args struct {
			uid   string
			quota Quota
		}
		tests := []struct {
			name    string
			args    args
			want    int
			wantErr bool
		}{
			{"PutUserBucketQuota should success", args{"quota-user", Quota{
				Enabled:    true,
				CheckOnRaw: false,
				MaxSizeKB:  100,
				MaxObjects: 100,
			}}, 200, false},
		}

		for _, tt := range tests {
			Convey(tt.name, func() {
				var buf bytes.Buffer
				err := json.NewEncoder(&buf).Encode(tt.args.quota)
				So(err, ShouldBeNil)

				got, err := rgw.PutUserBucketQuota(tt.args.uid, bytes.NewReader(buf.Bytes()))
				So(got, ShouldNotBeNil)
				So(got.StatusCode, ShouldEqual, tt.want)
				So(err, ShouldBeNil)
				t.Log(got)
			})
		}
	})
}

func TestRGWClient_PutBucketQuota(t *testing.T) {
	rgw := buildRGWClient(t)

	Convey("TestRGWClient_PutBucketQuota", t, func() {
		type args struct {
			uid    string
			bucket string
			quota  Quota
		}
		tests := []struct {
			name    string
			args    args
			want    int
			wantErr bool
		}{
			{"PutBucketQuota should success", args{"quota-user", "test-bkt", Quota{
				Enabled:    false,
				CheckOnRaw: false,
				MaxSizeKB:  200,
				MaxObjects: 100,
			}}, 200, false},
		}

		for _, tt := range tests {
			Convey(tt.name, func() {
				var buf bytes.Buffer
				err := json.NewEncoder(&buf).Encode(tt.args.quota)
				So(err, ShouldBeNil)

				got, err := rgw.PutBucketQuota(tt.args.uid, tt.args.bucket, bytes.NewReader(buf.Bytes()))
				So(got, ShouldNotBeNil)
				So(got.StatusCode, ShouldEqual, tt.want)
				So(err, ShouldBeNil)
				t.Log(got)
			})
		}
	})
}

func TestRGWClient_GetUserQuota(t *testing.T) {
	rgw := buildRGWClient(t)

	Convey("TestRGWClient_GetUserQuota", t, func() {
		type args struct {
			uid string
		}
		tests := []struct {
			name    string
			args    args
			want    bool
			wantErr bool
		}{
			{"GetUserQuota should success", args{"quota-user"}, true, false},
		}

		for _, tt := range tests {
			Convey(tt.name, func() {
				got, err := rgw.GetUserQuota(tt.args.uid)
				So(got, ShouldNotBeNil)
				So(got.Enabled, ShouldEqual, tt.want)
				So(err, ShouldBeNil)
				t.Log(got)
			})
		}
	})
}

func TestRGWClient_GetUserBucketQuota(t *testing.T) {
	rgw := buildRGWClient(t)

	Convey("TestRGWClient_GetUserBucketQuota", t, func() {
		type args struct {
			uid string
		}
		tests := []struct {
			name    string
			args    args
			want    bool
			wantErr bool
		}{
			{"GetUserBucketQuota should success", args{"quota-user"}, true, false},
		}

		for _, tt := range tests {
			Convey(tt.name, func() {
				got, err := rgw.GetUserBucketQuota(tt.args.uid)
				So(got, ShouldNotBeNil)
				So(got.Enabled, ShouldEqual, tt.want)
				So(err, ShouldBeNil)
				t.Log(got)
			})
		}
	})
}

func TestRGWClient_GetBucketInfo(t *testing.T) {
	rgw := buildRGWClient(t)

	Convey("TestRGWClient_GetUserBucketQuota", t, func() {
		type args struct {
			uid    string
			bucket string
		}
		tests := []struct {
			name    string
			args    args
			want    string
			wantErr bool
		}{
			{"GetBucketInfo should success", args{"quota-user", "test-bkt"}, "test-bkt", false},
		}

		for _, tt := range tests {
			Convey(tt.name, func() {
				got, err := rgw.GetBucketInfo(tt.args.uid, tt.args.bucket)
				So(got, ShouldNotBeNil)
				So((*got)[0].Bucket, ShouldEqual, tt.want)
				So(err, ShouldBeNil)
				t.Log(got)
			})
		}
	})
}

func TestRGWClient_GetUserInfo(t *testing.T) {
	rgw := buildRGWClient(t)

	Convey("TestRGWClient_GetUserInfo", t, func() {
		type args struct {
			uid   string
			stats string
		}
		tests := []struct {
			name    string
			args    args
			want    string
			wantErr bool
		}{
			{"GetUserInfo should has stats", args{"testid", "True"}, "testid", false},
		}

		for _, tt := range tests {
			Convey(tt.name, func() {
				got, err := rgw.GetUserInfo(tt.args.uid, tt.args.stats)
				So(got, ShouldNotBeNil)
				So(got.UserID, ShouldEqual, tt.want)
				So(got.Stats, ShouldNotBeNil)
				So(err, ShouldBeNil)
				t.Log(got)
			})
		}
	})
}

func TestRGWClient_CreateUser(t *testing.T) {
	rgw := buildRGWClient(t)

	Convey("TestRGWClient_CreateUser", t, func() {
		type args struct {
			userConf UserConf
		}
		tests := []struct {
			name    string
			args    args
			want    string
			wantErr bool
		}{
			{"CreateUser should success", args{UserConf{
				Uid:         "new-user",
				DisplayName: "dis-new-user",
				// Email:       "",
				// KeyType:     "",
				// AccessKey:   "",
				// SecretKey:   "",
				// UserCaps:    "",
				// GenerateKey: false,
				// Suspended:   0,
				MaxBuckets: 100,
				// Tenant:      "",
				// System:      false,
				// OpMask:      "read, write, delete",
			}}, "new-user", false},
		}

		for _, tt := range tests {
			Convey(tt.name, func() {
				got, err := rgw.CreateUser(&tt.args.userConf)
				So(got, ShouldNotBeNil)
				So(got.UserID, ShouldEqual, tt.want)
				So(err, ShouldBeNil)
				t.Log(got)
			})
		}
	})
}

func TestRGWClient_ModifyUser(t *testing.T) {
	rgw := buildRGWClient(t)

	Convey("TestRGWClient_ModifyUser", t, func() {
		type args struct {
			userConf UserConf
		}
		tests := []struct {
			name    string
			args    args
			want    string
			wantErr bool
		}{
			{"ModifyUser should success", args{UserConf{
				Uid:         "new-user",
				DisplayName: "dis-new-user",
				Email:       "abc@email.com",
				// KeyType:     "",
				// AccessKey:   "",
				// SecretKey:   "",
				// UserCaps:    "",
				// GenerateKey: false,
				// Suspended:   0,
				// MaxBuckets: 100,
				// Tenant:      "",
				// System:      false,
				// OpMask:      "read, write, delete",
			}}, "abc@email.com", false},
		}

		for _, tt := range tests {
			Convey(tt.name, func() {
				got, err := rgw.ModifyUser(&tt.args.userConf)
				So(got, ShouldNotBeNil)
				So(got.Email, ShouldEqual, tt.want)
				So(err, ShouldBeNil)
				t.Log(got)
			})
		}
	})
}

func TestRGWClient_RemoveUser(t *testing.T) {
	rgw := buildRGWClient(t)

	Convey("TestRGWClient_RemoveUser", t, func() {
		type args struct {
			uid string
		}
		tests := []struct {
			name    string
			args    args
			want    int
			wantErr bool
		}{
			{"RemoveUser should success", args{"new-user"}, 204, false},
		}

		for _, tt := range tests {
			Convey(tt.name, func() {
				got, err := rgw.RemoveUser(tt.args.uid)
				So(got, ShouldNotBeNil)
				So(got.StatusCode, ShouldEqual, tt.want)
				So(err, ShouldBeNil)
				t.Log(got)
			})
		}
	})
}

func TestRGWClient_CreateKey(t *testing.T) {
	rgw := buildRGWClient(t)

	Convey("TestRGWClient_CreateKey", t, func() {
		type args struct {
			userConf UserConf
		}
		tests := []struct {
			name    string
			args    args
			want    string
			wantErr bool
		}{
			{"CreateKey should success", args{UserConf{
				Uid:       "new-user",
				KeyType:   "s3",
				AccessKey: "jifdjiwhfiojidwpoopweru",
				SecretKey: "nufewpowqfnuifgqpwjfcnmhur",
			}}, "jifdjiwhfiojidwpoopweru", false},
		}

		for _, tt := range tests {
			Convey(tt.name, func() {
				got, err := rgw.CreateKey(&tt.args.userConf)
				So(got, ShouldNotBeNil)
				So((*got)[0].AccessKey, ShouldEqual, tt.want)
				So(err, ShouldBeNil)
				t.Log(got)
			})
		}
	})
}

func TestRGWClient_RemoveKey(t *testing.T) {
	rgw := buildRGWClient(t)

	Convey("TestRGWClient_RemoveKey", t, func() {
		type args struct {
			userConf UserConf
		}
		tests := []struct {
			name    string
			args    args
			want    int
			wantErr bool
		}{
			{"RemoveKey should success", args{UserConf{
				Uid:       "new-user",
				KeyType:   "s3",
				AccessKey: "jifdjiwhfiojidwpoopweru",
			}}, 204, false},
		}

		for _, tt := range tests {
			Convey(tt.name, func() {
				got, err := rgw.RemoveKey(&tt.args.userConf)
				So(got, ShouldNotBeNil)
				So(got.StatusCode, ShouldEqual, tt.want)
				So(err, ShouldBeNil)
				t.Log(got)
			})
		}
	})
}

func TestRGWClient_AddCaps(t *testing.T) {
	rgw := buildRGWClient(t)

	Convey("TestRGWClient_AddCaps", t, func() {
		type args struct {
			uid  string
			caps string
		}
		tests := []struct {
			name    string
			args    args
			want    string
			wantErr bool
		}{
			{"AddCaps should success", args{"new-user", "users=write"}, "write", false},
		}

		for _, tt := range tests {
			Convey(tt.name, func() {
				got, err := rgw.AddCaps(tt.args.uid, tt.args.caps)
				So(got, ShouldNotBeNil)
				So((*got)[0].Perm, ShouldEqual, tt.want)
				So(err, ShouldBeNil)
				t.Log(got)
			})
		}
	})
}

func TestRGWClient_RemoveCaps(t *testing.T) {
	rgw := buildRGWClient(t)

	Convey("TestRGWClient_RemoveCaps", t, func() {
		type args struct {
			uid  string
			caps string
		}
		tests := []struct {
			name    string
			args    args
			want    string
			wantErr bool
		}{
			{"RemoveCaps should success", args{"new-user", "users=write"}, "write", false},
		}

		for _, tt := range tests {
			Convey(tt.name, func() {
				got, err := rgw.RemoveCaps(tt.args.uid, tt.args.caps)
				So(got, ShouldNotBeNil)
				So((*got)[0].Perm, ShouldEqual, tt.want)
				So(err, ShouldBeNil)
				t.Log(got)
			})
		}
	})
}
