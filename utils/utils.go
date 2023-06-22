package utils

import (
	"fmt"
	"io"
	"net/http"
)

func GetSeekerLength(s io.Seeker) (int64, error) {
	currentPos, err := s.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	defer s.Seek(currentPos, io.SeekStart)

	endPos, err := s.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}
	defer s.Seek(currentPos, io.SeekStart)

	return endPos - currentPos, nil
}

func SetContentLengthHeader(req *http.Request, body io.ReadSeeker) error {
	if body != nil {
		length, err := GetSeekerLength(body)
		if err != nil {
			return err
		}
		if length > 0 {
			req.ContentLength = length
			req.Header.Set("Content-Length", fmt.Sprintf("%d", length))
		} else {
			req.ContentLength = 0
			req.Header.Del("Content-Length")
		}
	}
	return nil
}
