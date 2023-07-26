package radosgw

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/beevik/etree"
)

type (
	MetaDataFilter map[string]string
	TagFilter      map[string]string
)

type ListTopicsResponse struct {
	XMLName          xml.Name `xml:"ListTopicsResponse"`
	Text             string   `xml:",chardata"`
	Xmlns            string   `xml:"xmlns,attr"`
	ListTopicsResult struct {
		Text   string `xml:",chardata"`
		Topics struct {
			Text   string `xml:",chardata"`
			Member []struct {
				Text     string `xml:",chardata"`
				User     string `xml:"User"`
				Name     string `xml:"Name"`
				EndPoint struct {
					Text            string `xml:",chardata"`
					EndpointAddress string `xml:"EndpointAddress"`
					EndpointArgs    string `xml:"EndpointArgs"`
					EndpointTopic   string `xml:"EndpointTopic"`
				} `xml:"EndPoint"`
				TopicArn string `xml:"TopicArn"`
			} `xml:"member"`
		} `xml:"Topics"`
	} `xml:"ListTopicsResult"`
	ResponseMetadata struct {
		Text      string `xml:",chardata"`
		RequestId string `xml:"RequestId"`
	} `xml:"ResponseMetadata"`
}

type GetTopicResponse struct {
	XMLName        xml.Name `xml:"GetTopicResponse"`
	Text           string   `xml:",chardata"`
	GetTopicResult struct {
		Text  string `xml:",chardata"`
		Topic struct {
			Text     string `xml:",chardata"`
			User     string `xml:"User"`
			Name     string `xml:"Name"`
			EndPoint struct {
				Text            string `xml:",chardata"`
				EndpointAddress string `xml:"EndpointAddress"`
				EndpointArgs    string `xml:"EndpointArgs"`
				EndpointTopic   string `xml:"EndpointTopic"`
				HasStoredSecret string `xml:"HasStoredSecret"`
				Persistent      string `xml:"Persistent"`
			} `xml:"EndPoint"`
			TopicArn   string `xml:"TopicArn"`
			OpaqueData string `xml:"OpaqueData"`
		} `xml:"Topic"`
	} `xml:"GetTopicResult"`
	ResponseMetadata struct {
		Text      string `xml:",chardata"`
		RequestId string `xml:"RequestId"`
	} `xml:"ResponseMetadata"`
}

type CreateTopicResponse struct {
	XMLName           xml.Name `xml:"CreateTopicResponse"`
	Text              string   `xml:",chardata"`
	Xmlns             string   `xml:"xmlns,attr"`
	CreateTopicResult struct {
		Text     string `xml:",chardata"`
		TopicArn string `xml:"TopicArn"`
	} `xml:"CreateTopicResult"`
	ResponseMetadata struct {
		Text      string `xml:",chardata"`
		RequestId string `xml:"RequestId"`
	} `xml:"ResponseMetadata"`
}

type NotificationConfiguration struct {
	XMLName            xml.Name `xml:"NotificationConfiguration"`
	Text               string   `xml:",chardata"`
	Xmlns              string   `xml:"xmlns,attr"`
	TopicConfiguration []struct {
		Text   string   `xml:",chardata"`
		ID     string   `xml:"Id"`
		Topic  string   `xml:"Topic"`
		Event  []string `xml:"Event"`
		Filter struct {
			Text  string `xml:",chardata"`
			S3Key struct {
				Text       string `xml:",chardata"`
				FilterRule []struct {
					Text  string `xml:",chardata"`
					Name  string `xml:"Name"`
					Value string `xml:"Value"`
				} `xml:"FilterRule"`
			} `xml:"S3Key"`
			S3Metadata struct {
				Text       string `xml:",chardata"`
				FilterRule []struct {
					Text  string `xml:",chardata"`
					Name  string `xml:"Name"`
					Value string `xml:"Value"`
				} `xml:"FilterRule"`
			} `xml:"S3Metadata"`
			S3Tags struct {
				Text       string `xml:",chardata"`
				FilterRule []struct {
					Text  string `xml:",chardata"`
					Name  string `xml:"Name"`
					Value string `xml:"Value"`
				} `xml:"FilterRule"`
			} `xml:"S3Tags"`
		} `xml:"Filter"`
	} `xml:"TopicConfiguration"`
}

func (rgw *RgwClient) ListTopics() (*ListTopicsResponse, error) {
	body := strings.NewReader("Action=ListTopics&Version=2010-03-31")
	url := fmt.Sprintf("%s/", *rgw.config.Endpoint)
	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return nil, err
	}

	resp, err := rgw.buildSignerV2AndSendReq(req)
	if err != nil {
		return nil, err
	}

	buff, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var topics ListTopicsResponse
	err = xml.Unmarshal(buff, &topics)
	if err != nil {
		return nil, err
	}

	return &topics, nil
}

func (rgw *RgwClient) CreateTopic(topicName, pushEndpoint string) (string, error) {
	body := fmt.Sprintf("Action=CreateTopic&Version=2010-03-31&Name=%s&push-endpoint=%s", topicName, pushEndpoint)
	url := fmt.Sprintf("%s/", *rgw.config.Endpoint)
	req, err := http.NewRequest("POST", url, strings.NewReader(body))
	if err != nil {
		return "", err
	}

	resp, err := rgw.buildSignerV2AndSendReq(req)
	if err != nil {
		return "", err
	}

	buff, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	var topic CreateTopicResponse
	err = xml.Unmarshal(buff, &topic)
	if err != nil {
		return "", err
	}

	return topic.CreateTopicResult.TopicArn, nil
}

func (rgw *RgwClient) GetTopic(topicArn string) (*GetTopicResponse, error) {
	body := fmt.Sprintf("Action=GetTopic&Version=2010-03-31&TopicArn=%s", topicArn)
	url := fmt.Sprintf("%s/", *rgw.config.Endpoint)
	req, err := http.NewRequest("POST", url, strings.NewReader(body))
	if err != nil {
		return nil, err
	}

	resp, err := rgw.buildSignerV2AndSendReq(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == 404 {
		return nil, nil
	}

	buff, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var topic GetTopicResponse
	err = xml.Unmarshal(buff, &topic)
	if err != nil {
		return nil, err
	}

	return &topic, nil
}

func (rgw *RgwClient) DeleteTopic(topicArn string) (*http.Response, error) {
	body := fmt.Sprintf("Action=DeleteTopic&Version=2010-03-31&TopicArn=%s", topicArn)
	url := fmt.Sprintf("%s/", *rgw.config.Endpoint)
	req, err := http.NewRequest("POST", url, strings.NewReader(body))
	if err != nil {
		return nil, err
	}

	resp, err := rgw.buildSignerV2AndSendReq(req)

	return resp, err
}

// CreateNotification TODO Support tag and regular expression filtering
func (rgw *RgwClient) CreateNotification(topicArn, bucket, notificationId, prefix, suffix string, metaData MetaDataFilter, events []string) (*http.Response, error) {
	body, err := rgw.buildNotificationBody(topicArn, notificationId, prefix, suffix, metaData, events)
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("%s/%s?notification", *rgw.config.Endpoint, bucket)
	req, err := http.NewRequest("PUT", url, strings.NewReader(body))
	if err != nil {
		return nil, err
	}

	resp, err := rgw.buildSignerV2AndSendReq(req)

	return resp, err
}

// GetNotification TODO define a struct to hold response for easier use by the client
func (rgw *RgwClient) GetNotification(bucket, notificationId string) (*NotificationConfiguration, error) {
	if bucket == "" {
		err := errors.New("bucket can not be empty")
		return nil, err
	}

	url := fmt.Sprintf("%s/%s?notification", *rgw.config.Endpoint, bucket)
	if notificationId != "" {
		url += "=" + notificationId
	}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := rgw.buildSignerV2AndSendReq(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == 404 {
		return nil, nil
	}

	buff, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var notif NotificationConfiguration
	err = xml.Unmarshal(buff, &notif)
	if err != nil {
		return nil, err
	}

	return &notif, nil
}

func (rgw *RgwClient) DeleteNotification(bucket, notificationId string) (*http.Response, error) {
	url := fmt.Sprintf("%s/%s?notification=%s", *rgw.config.Endpoint, bucket, notificationId)
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := rgw.buildSignerV2AndSendReq(req)

	return resp, err
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
