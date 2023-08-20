package s3box

import (
	"fmt"
	"testing"
)

func TestBoxClient_PostObject(t *testing.T) {
	svc := buildS3Client(t)
	bc := NewBoxClient(svc)
	response, err := bc.PostObject("bkt", "abc", "2023-08-04T00:00:00Z", "./abc.txt")
	if err != nil {
		return
	}
	fmt.Println(response)
}
