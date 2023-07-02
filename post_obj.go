package s3box

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"strings"
	"time"
)

// PostObject the expirationTime should use a format similar to "2022-06-04T00:00:00Z"
func (bc *BoxClient) PostObject(bucketName, objKey, expirationTime, filePath string) (*http.Response, error) {
	v, err := bc.svc.Config.Credentials.Get()
	if err != nil {
		return nil, err
	}
	currentTime := time.Now()
	currentDate := currentTime.Format("20060102")
	xAmzDate := currentTime.Format("20060102T150405Z")
	credential := fmt.Sprintf("%s/%s/%s/s3/aws4_request", v.AccessKeyID, currentDate, "region")

	algorithm := "AWS4-HMAC-SHA256"
	policy := fmt.Sprintf(`{"expiration": "%s",
		"conditions": [
			{"bucket": "%s" },
			{"key": "%s"},
			{"x-amz-algorithm":"%s"},
			{"x-amz-credential":"%s"},
			{"x-amz-date":"%s"}
		]}`, expirationTime, bucketName, objKey, algorithm, credential, xAmzDate)

	encodePolicy := base64.StdEncoding.EncodeToString([]byte(policy))
	signature := bc.getSignatureKey(v.SecretAccessKey, currentDate, "region", "s3", encodePolicy)

	field := map[string]string{
		"key":              objKey,
		"x-amz-algorithm":  algorithm,
		"x-amz-credential": credential,
		"x-amz-date":       xAmzDate,
		"Policy":           encodePolicy,
		"X-Amz-Signature":  signature,
	}

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	for key, value := range field {
		err = writer.WriteField(key, value)
		if err != nil {
			return nil, err
		}
	}

	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	part, err := writer.CreateFormFile("file", objKey)
	if err != nil {
		return nil, err
	}

	_, err = io.Copy(part, file)
	if err != nil {
		return nil, err
	}

	err = writer.Close()
	if err != nil {
		return nil, err
	}

	endpoint := strings.TrimRight(*bc.svc.Config.Endpoint, "/")
	url := endpoint + "/" + bucketName
	req, err := http.NewRequest("POST", url, body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	client := &http.Client{}
	resp, err := client.Do(req)

	return resp, err
}

func (bc *BoxClient) calculateHMACSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

func (bc *BoxClient) getSignatureKey(secretKey, dateStamp, region, service, encodePolicy string) string {
	dateKey := bc.calculateHMACSHA256([]byte("AWS4"+secretKey), []byte(dateStamp))
	regionKey := bc.calculateHMACSHA256(dateKey, []byte(region))
	serviceKey := bc.calculateHMACSHA256(regionKey, []byte(service))
	signingKey := bc.calculateHMACSHA256(serviceKey, []byte("aws4_request"))
	signatureBytes := bc.calculateHMACSHA256(signingKey, []byte(encodePolicy))
	return string(signatureBytes)
}
