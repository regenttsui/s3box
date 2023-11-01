package s3box

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"strings"
)

// PostObject use form to upload object, fields should include all the necessary and optional fields except for x-amz-signature
func (bc *BoxClient) PostObject(bucketName, objKey, date, encodedPolicy, region, filePath string, fields map[string]string) (*http.Response, error) {
	v, err := bc.svc.Config.Credentials.Get()
	if err != nil {
		return nil, err
	}
	signature := bc.getSignatureKey(v.SecretAccessKey, date, region, "s3", encodedPolicy)
	fields["x-amz-signature"] = signature

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	for key, value := range fields {
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
	resp, err := bc.httpClient.Do(req)

	return resp, err
}

func (bc *BoxClient) calculateHMACSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

func (bc *BoxClient) getSignatureKey(secretKey, dateStamp, region, service, encodedPolicy string) string {
	dateKey := bc.calculateHMACSHA256([]byte("AWS4"+secretKey), []byte(dateStamp))
	regionKey := bc.calculateHMACSHA256(dateKey, []byte(region))
	serviceKey := bc.calculateHMACSHA256(regionKey, []byte(service))
	signingKey := bc.calculateHMACSHA256(serviceKey, []byte("aws4_request"))
	signatureBytes := bc.calculateHMACSHA256(signingKey, []byte(encodedPolicy))
	return hex.EncodeToString(signatureBytes)
}
