package utils

import (
	"fmt"
	"io"
	"net/http"
	"strings"
)

const doubleSpace = "  "

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

// StripExcessSpaces will rewrite the passed in slice's string values to not
// contain multiple side-by-side spaces.
func StripExcessSpaces(vals []string) {
	var j, k, l, m, spaces int
	for i, str := range vals {
		// Trim trailing spaces
		for j = len(str) - 1; j >= 0 && str[j] == ' '; j-- {
		}

		// Trim leading spaces
		for k = 0; k < j && str[k] == ' '; k++ {
		}
		str = str[k : j+1]

		// Strip multiple spaces.
		j = strings.Index(str, doubleSpace)
		if j < 0 {
			vals[i] = str
			continue
		}

		buf := []byte(str)
		for k, m, l = j, j, len(buf); k < l; k++ {
			if buf[k] == ' ' {
				if spaces == 0 {
					// First space.
					buf[m] = buf[k]
					m++
				}
				spaces++
			} else {
				// End of multiple spaces.
				spaces = 0
				buf[m] = buf[k]
				m++
			}
		}

		vals[i] = string(buf[:m])
	}
}
