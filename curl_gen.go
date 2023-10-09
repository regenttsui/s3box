package s3box

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"net/http"
	"strings"
	"time"
)

type BodyType int

const (
	NoBody BodyType = iota
	JSONBody
	XMLBody
	BinaryBody
)

// CurlGen generate curl command to send s3 v2 request in terminal.
// It does not support ipv6 address, upload/download objects.
// The argument body should be an empty/json/xml string, url should be path style
func CurlGen(conf *aws.Config, method, url, body, md5 string, bodyType BodyType) string {
	if body != "" && method != "PUT" && method != "POST" {
		return "only POST and PUT request should have body"
	}
	var builder strings.Builder
	builder.WriteString("curl -i -v -X ")
	switch method {
	case "HEAD":
		builder.WriteString("HEAD ")
	case "GET":
		builder.WriteString("GET ")
	case "PUT":
		builder.WriteString("PUT ")
	case "POST":
		builder.WriteString("POST ")
	case "DELETE":
		builder.WriteString("DELETE ")
	default:
		return "invalid method"
	}

	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		return err.Error()
	}

	if body != "" {
		switch bodyType {
		case JSONBody:
			req.Header.Set("Content-Type", "application/json")
		case XMLBody:
			req.Header.Set("Content-Type", "application/xml")
		default:
			return "unsupported body type"
		}
		builder.WriteString(fmt.Sprintf("-H \"Content-Type: %s\" ", req.Header.Get("Content-Type")))
	}

	if md5 != "" {
		req.Header.Set("Content-MD5", md5)
		builder.WriteString(fmt.Sprintf("-H \"Content-MD5: %s\" ", md5))
	}

	signer := NewSigner(*conf, time.Now())
	err = signer.Sign(req)
	if err != nil {
		return err.Error()
	}

	HdrsAndUrl := fmt.Sprintf("-H \"Date: %s\" -H \"Authorization: %s\" \"%s\"", req.Header.Get("Date"), req.Header.Get("Authorization"), url)
	builder.WriteString(HdrsAndUrl)
	if body != "" {
		data := fmt.Sprintf(" -d '%s'", body)
		builder.WriteString(data)
	}

	return builder.String()
}
