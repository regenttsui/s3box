/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

 package s3box

 import (
	 "crypto/hmac"
	 "crypto/sha1"
	 "encoding/base64"
	 "fmt"
	 "github.com/aws/aws-sdk-go/aws/request"
	 "github.com/regenttsui/s3box/utils"
	 "net/http"
	 "net/url"
	 "regexp"
	 "sort"
	 "strings"
	 "time"
 
	 "github.com/aws/aws-sdk-go/aws"
	 "github.com/aws/aws-sdk-go/aws/credentials"
 )
 
 const (
	 timeFormat      = time.RFC1123Z
	 amzHeaderPrefix = "x-amz"
 )
 
 const logSignInfoMsg = `DEBUG: Request Signature:
 ---[ STRING TO SIGN ]--------------------------------
 %s
 ---[ SIGNATURE ]-------------------------------------
 %s
 -----------------------------------------------------`
 
 type Signer struct {
	 Time        time.Time
	 Credentials *credentials.Credentials
	 Debug       aws.LogLevelType
	 Logger      aws.Logger
 
	 Query        url.Values
	 stringToSign string
	 signature    string
 }
 
 // NewSigner returns a Signer pointer configured with the aws.Config and time.Time
 func NewSigner(config aws.Config, time time.Time) *Signer {
	 v2 := &Signer{
		 Time:        time,
		 Credentials: config.Credentials,
		 Debug:       config.LogLevel.Value(),
		 Logger:      config.Logger,
	 }
 
	 return v2
 }
 
 func (v2 *Signer) Sign(r *http.Request) error {
	 credValue, err := v2.Credentials.Get()
	 if err != nil {
		 return err
	 }
 
	 r.Header.Set("Date", v2.Time.UTC().Format(timeFormat))
	 if credValue.SessionToken != "" {
		 r.Header.Set("X-Amz-Security-Token", credValue.SessionToken)
	 }
 
	 request.SanitizeHostForHeader(r)
	 if r.URL.Path == "" {
		 r.URL.Path += "/"
	 }
 
	 v2.stringToSign = v2.buildStringToSign(r)
 
	 hash := hmac.New(sha1.New, []byte(credValue.SecretAccessKey))
	 hash.Write([]byte(v2.stringToSign))
	 v2.signature = base64.StdEncoding.EncodeToString(hash.Sum(nil))
	 authHeader := fmt.Sprintf("AWS %s:%s", credValue.AccessKeyID, v2.signature)
	 r.Header.Set("Authorization", authHeader)
 
	 if v2.Debug.Matches(aws.LogDebugWithSigning) {
		 v2.logSigningInfo()
	 }
 
	 return nil
 }
 
 func (v2 *Signer) buildStringToSign(r *http.Request) string {
	 str := strings.Join([]string{
		 r.Method,
		 r.Header.Get("Content-MD5"),
		 r.Header.Get("Content-Type"),
		 r.Header.Get("Date"),
	 }, "\n")
	 str += "\n"
	 if canonicalHeaders := v2.canonicalizedAmzHeaders(r); canonicalHeaders != "" {
		 str += canonicalHeaders
	 }
	 str += v2.canonicalizedResource(r)
 
	 return str
 }
 
 func (v2 *Signer) canonicalizedAmzHeaders(r *http.Request) string {
	 var headers []string
	 signedHeaderVals := make(http.Header)
 
	 for k, v := range r.Header {
		 lowerCaseKey := strings.ToLower(k)
		 if !strings.HasPrefix(lowerCaseKey, amzHeaderPrefix) {
			 continue // ignored header
		 }
 
		 if _, ok := signedHeaderVals[lowerCaseKey]; ok {
			 // include additional values
			 signedHeaderVals[lowerCaseKey] = append(signedHeaderVals[lowerCaseKey], v...)
			 continue
		 }
 
		 headers = append(headers, lowerCaseKey)
		 signedHeaderVals[lowerCaseKey] = v
	 }
	 sort.Strings(headers)
 
	 headerItems := make([]string, len(headers))
	 for i, k := range headers {
		 if k == "host" {
			 if r.Host != "" {
				 headerItems[i] = "host:" + r.Host
			 } else {
				 headerItems[i] = "host:" + r.URL.Host
			 }
		 } else {
			 headerValues := make([]string, len(signedHeaderVals[k]))
			 for j, v := range signedHeaderVals[k] {
				 headerValues[j] = strings.TrimSpace(v)
			 }
			 headerItems[i] = k + ":" +
				 strings.Join(headerValues, ",")
		 }
	 }
	 utils.StripExcessSpaces(headerItems)
	 if len(headerItems) > 0 {
		 return strings.Join(headerItems, "\n") + "\n"
	 }
	 return ""
 }
 
 func (v2 *Signer) canonicalizedResource(r *http.Request) string {
	 resource := ""
 
	 if strings.Count(r.Host, ".") == 4 {
		 bucketName := strings.Split(r.Host, ".")[0]
		 resource += "/" + bucketName
	 }
 
	 resource += r.URL.EscapedPath()
 
	 sortedS3Subresources := []string{
		 "acl", "cors", "delete", "lifecycle", "location",
		 "logging", "notification", "partNumber",
		 "policy", "requestPayment",
		 "response-cache-control",
		 "response-content-disposition",
		 "response-content-encoding",
		 "response-content-language",
		 "response-content-type",
		 "response-expires",
		 "torrent", "uploadId", "uploads", "versionId",
		 "versioning", "versions", "website",
	 }
	 requestQuery := r.URL.Query()
	 encodedQuery := ""
	 for _, q := range sortedS3Subresources {
		 if values, ok := requestQuery[q]; ok {
			 for _, v := range values {
				 if encodedQuery != "" {
					 encodedQuery += "&"
				 }
				 if v == "" {
					 encodedQuery += q
				 } else {
					 encodedQuery += q + "=" + v
				 }
			 }
		 }
	 }
	 if encodedQuery != "" {
		 resource += "?" + encodedQuery
	 }
 
	 return resource
 }
 
 func (v2 *Signer) containsChinese(str string) bool {
	 result, _ := regexp.MatchString(`[\x{4e00}-\x{9fa5}]+`, str)
	 return result
 }
 
 func (v2 *Signer) logSigningInfo() {
	 msg := fmt.Sprintf(logSignInfoMsg, v2.stringToSign, v2.signature)
	 v2.Logger.Log(msg)
 }
 