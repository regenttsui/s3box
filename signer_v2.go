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
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
)

const (
	signatureVersion = "2"
	signatureMethod  = "HmacSHA256"
	timeFormat       = "2006-01-02T15:04:05Z"
)

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

	if r.Method == "POST" {
		// Parse the HTTP request to obtain the query parameters that will
		// be used to build the string to sign. Note that because the HTTP
		// request will need to be modified, the PostForm and Form properties
		// are reset to nil after parsing.
		r.ParseForm()
		v2.Query = r.PostForm
		r.PostForm = nil
		r.Form = nil
	} else {
		v2.Query = r.URL.Query()
	}

	// Set new query parameters
	v2.Query.Set("AWSAccessKeyId", credValue.AccessKeyID)
	v2.Query.Set("SignatureVersion", signatureVersion)
	v2.Query.Set("SignatureMethod", signatureMethod)
	v2.Query.Set("Timestamp", v2.Time.UTC().Format(timeFormat))
	if credValue.SessionToken != "" {
		v2.Query.Set("SecurityToken", credValue.SessionToken)
	}

	// in case this is a retry, ensure no signature present
	v2.Query.Del("Signature")

	method := r.Method
	host := r.URL.Host
	path := r.URL.Path
	if path == "" {
		path = "/"
	}

	// obtain all the query keys and sort them
	queryKeys := make([]string, 0, len(v2.Query))
	for key := range v2.Query {
		queryKeys = append(queryKeys, key)
	}
	sort.Strings(queryKeys)

	// build URL-encoded query keys and values
	queryKeysAndValues := make([]string, len(queryKeys))
	for i, key := range queryKeys {
		k := strings.Replace(url.QueryEscape(key), "+", "%20", -1)
		v := strings.Replace(url.QueryEscape(v2.Query.Get(key)), "+", "%20", -1)
		queryKeysAndValues[i] = k + "=" + v
	}

	// join into one query string
	query := strings.Join(queryKeysAndValues, "&")

	// build the canonical string for the V2 signature
	v2.stringToSign = strings.Join([]string{
		method,
		host,
		path,
		query,
	}, "\n")

	hash := hmac.New(sha256.New, []byte(credValue.SecretAccessKey))
	hash.Write([]byte(v2.stringToSign))
	v2.signature = base64.StdEncoding.EncodeToString(hash.Sum(nil))
	v2.Query.Set("Signature", v2.signature)
	r.URL.RawQuery = v2.Query.Encode()

	if v2.Debug.Matches(aws.LogDebugWithSigning) {
		v2.logSigningInfo()
	}

	return nil
}

const logSignInfoMsg = `DEBUG: Request Signature:
---[ STRING TO SIGN ]--------------------------------
%s
---[ SIGNATURE ]-------------------------------------
%s
-----------------------------------------------------`

func (v2 *Signer) logSigningInfo() {
	msg := fmt.Sprintf(logSignInfoMsg, v2.stringToSign, v2.Query.Get("Signature"))
	v2.Logger.Log(msg)
}
