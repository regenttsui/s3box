package radosgw

import (
	"errors"
	"fmt"
	"github.com/beevik/etree"
	"io"
	"net/http"
	"strings"
)

type (
	MetaDataFilter map[string]string
	TagFilter      map[string]string
)

// ListTopics TODO define a struct to hold response for easier use by the client
func (rgw *RgwClient) ListTopics() (*http.Response, error) {
	body := strings.NewReader("Action=ListTopics&Version=2010-03-31")
	url := fmt.Sprintf("%s/", *rgw.config.Endpoint)
	//FIXME body length?
	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return nil, err
	}

	resp, err := rgw.buildSignerV2AndSendReq(req)

	return resp, err
}

func (rgw *RgwClient) CreateTopic(topicName, pushEndpoint string) (string, error) {
	body := fmt.Sprintf("Action=CreateTopic&Version=2010-03-31&Name=%s&push-endpoint=%s", topicName, pushEndpoint)
	url := fmt.Sprintf("%s/", *rgw.config.Endpoint)
	req, err := http.NewRequest("POST", url, strings.NewReader(body))
	if err != nil {
		return "", err
	}

	resp, err := rgw.buildSignerV2AndSendReq(req)

	buff, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	topicArn, err := rgw.parseCreateTopicResp(buff)
	if err != nil {
		return "", err
	}

	return topicArn, nil
}

func (rgw *RgwClient) DeleteTopic(topicArn string) (*http.Response, error) {
	body := fmt.Sprintf("Action=DeleteTopic&Version=2010-03-31&TopicArn=%s", topicArn)
	url := fmt.Sprintf("%s/", *rgw.config.Endpoint)
	req, err := http.NewRequest("POST", url, strings.NewReader(body))
	if err != nil {
		return nil, err
	}

	resp, err := rgw.buildSignerV2AndSendReq(req)

	return resp, nil
}

// CreateNotification TODO Support tag and regular expression filtering
func (rgw *RgwClient) CreateNotification(topicArn, bucket, notificationId, prefix, suffix string, metaData MetaDataFilter, events []string) (*http.Response, error) {
	body, err := rgw.buildNotificationBody(topicArn, notificationId, prefix, suffix, metaData, events)
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("%s/%s?notification", *rgw.config.Endpoint, bucket)
	req, err := http.NewRequest("POST", url, strings.NewReader(body))
	if err != nil {
		return nil, err
	}

	resp, err := rgw.buildSignerV2AndSendReq(req)

	return resp, nil
}

// GetNotification TODO define a struct to hold response for easier use by the client
func (rgw *RgwClient) GetNotification(bucket, notificationId string) (*http.Response, error) {
	if bucket == "" {
		err := errors.New("bucket can not be empty")
		return nil, err
	}

	url := fmt.Sprintf("%s/%s?notification", *rgw.config.Endpoint, bucket)
	if notificationId != "" {
		url += "=" + notificationId
	}
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := rgw.buildSignerV2AndSendReq(req)

	return resp, nil
}

func (rgw *RgwClient) DeleteNotification(bucket, notificationId string) (*http.Response, error) {
	url := fmt.Sprintf("%s/%s?notification=%s", *rgw.config.Endpoint, bucket, notificationId)
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := rgw.buildSignerV2AndSendReq(req)

	return resp, nil
}

func (rgw *RgwClient) parseCreateTopicResp(data []byte) (string, error) {
	doc := etree.NewDocument()
	if err := doc.ReadFromBytes(data); err != nil {
		return "", err
	}

	ElemCreateTopicResponse := doc.SelectElement("CreateTopicResponse")
	ElemCreateTopicResult := ElemCreateTopicResponse.SelectElement("CreateTopicResult")
	ElemTopicArn := ElemCreateTopicResult.SelectElement("TopicArn")
	topicArn := ElemTopicArn.Text()
	return topicArn, nil
}

// TODO Support tag and regular expression filtering
func (rgw *RgwClient) buildNotificationBody(topicArn, NotificationId, prefix, suffix string, metaData MetaDataFilter, events []string) (string, error) {
	doc := etree.NewDocument()

	doc.CreateProcInst("xml", `version="1.0" encoding="UTF-8"`)

	notificationConfiguration := doc.CreateElement("NotificationConfiguration")
	topicConfiguration := notificationConfiguration.CreateElement("TopicConfiguration")

	id := topicConfiguration.CreateElement("Id")
	id.CreateText(NotificationId)

	topic := topicConfiguration.CreateElement("Topic")
	topic.CreateText(topicArn)
	for _, e := range events {
		event := topicConfiguration.CreateElement("Event")
		event.CreateText(e)
	}
	filter := topicConfiguration.CreateElement("Filter")
	s3Key := filter.CreateElement("S3Key")
	if prefix != "" {
		filterRule := s3Key.CreateElement("FilterRule")

		name := filterRule.CreateElement("Name")
		name.CreateText("prefix")
		value := filterRule.CreateElement("Value")
		value.CreateText(prefix)
	}
	if suffix != "" {
		filterRule := s3Key.CreateElement("FilterRule")

		name := filterRule.CreateElement("Name")
		name.CreateText("suffix")
		value := filterRule.CreateElement("Value")
		value.CreateText(suffix)
	}

	if len(metaData) > 0 {
		s3Metadata := filter.CreateElement("S3Metadata")
		for k, v := range metaData {
			filterRule := s3Metadata.CreateElement("FilterRule")

			name := filterRule.CreateElement("Name")
			name.CreateText(k)
			value := filterRule.CreateElement("Value")
			value.CreateText(v)
		}
	}

	buff, err := doc.WriteToBytes()
	if err != nil {
		return "", err
	}

	return string(buff), nil
}
