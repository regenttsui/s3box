package S3Box

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// BucketCleaner
// TODO modify print of result and error
type BucketCleaner struct {
	svc          *s3.S3
	deletedCount uint64
	listedCount  uint64
}

func NewBucketCleaner(svc *s3.S3) *BucketCleaner {
	c := &BucketCleaner{
		svc:          svc,
		deletedCount: 0,
		listedCount:  0,
	}
	return c
}

// EmptyBucket is to empty objects in the bucket concurrently.
// objChanCap is the capacity of channel to store the objects from listObjs
// multiDelNum decides whether to delete multiple objects in a request
// TODO 增加清理分片
func (c *BucketCleaner) EmptyBucket(bucketName string, deleteWorkerNum, objChanCap, multiDelNum int, deleteBucket bool) error {
	var wg sync.WaitGroup
	wg.Add(deleteWorkerNum + 2)
	startTime := time.Now()
	go c.crontabPrintResults(startTime)
	// objChannel存放实际的对象名
	objChannel := make(chan s3.ObjectIdentifier, objChanCap)
	// listObjs并将对象名放入objChannel
	go func() {
		defer wg.Done()
		c.listObjs(objChannel, bucketName)
	}()
	go func() {
		defer wg.Done()
		c.abortAllMultiparts(bucketName)
	}()

	// 并发删除对象
	for i := 0; i < deleteWorkerNum; i++ {
		if multiDelNum > 0 {
			go func() {
				defer wg.Done()
				c.deleteObjs(objChannel, bucketName, multiDelNum)
			}()
		} else {
			go func() {
				defer wg.Done()
				c.deleteObj(objChannel, bucketName)
			}()
		}
	}

	wg.Wait()

	if deleteBucket {
		deleteBucketInput := &s3.DeleteBucketInput{
			Bucket: aws.String(bucketName),
		}
		_, err := c.svc.DeleteBucket(deleteBucketInput)
		if err != nil {
			return err
		}
	}
	return nil
}

// DeleteAllBuckets delete all buckets or all buckets contain a specified string of a user
func (c *BucketCleaner) DeleteAllBuckets(containedStr string) error {
	listBucketsInput := &s3.ListBucketsInput{}
	listBucketsOutput, err := c.svc.ListBuckets(listBucketsInput)
	if err != nil {
		return err
	}
	for _, b := range listBucketsOutput.Buckets {
		if containedStr != "" {
			if strings.Contains(*b.Name, containedStr) {
				go c.EmptyBucket(*b.Name, 3, 1000, 1000, true)
			}
		} else {
			go c.EmptyBucket(*b.Name, 3, 1000, 1000, true)
		}
	}
	return nil
}

// 定时打印任务+每秒更新状态
func (c *BucketCleaner) crontabPrintResults(startTime time.Time) {
	timeTicker := time.NewTicker(time.Duration(1) * time.Second)
	tps := 0.0
	for {
		<-timeTicker.C
		timeDiff := time.Since(startTime)
		tps = float64(c.deletedCount) / timeDiff.Seconds()
		fmt.Printf("TPS:%f\n", tps)
		//if {
		//	timeTicker.Stop()
		//}
	}
}

func (c *BucketCleaner) deleteObjs(objChannel chan s3.ObjectIdentifier, bucketName string, multiDelNum int) {
	objs := make([]*s3.ObjectIdentifier, 0, multiDelNum)
	for obj := range objChannel {
		objId := s3.ObjectIdentifier{
			Key:       obj.Key,
			VersionId: obj.VersionId,
		}
		objs = append(objs, &objId)
		if len(objs) < multiDelNum {
			continue
		}
		deleteObjectsInput := &s3.DeleteObjectsInput{
			Bucket: aws.String(bucketName),
			Delete: &s3.Delete{
				Objects: objs,
				Quiet:   aws.Bool(true),
			},
		}

		_, err := c.svc.DeleteObjects(deleteObjectsInput)
		if err != nil {
			fmt.Printf("Failed to delete objects. %v\n", err)
		} else {
			fmt.Printf("deleted %d objects\n", len(objs))
			atomic.AddUint64(&c.deletedCount, 1)
		}

		objs = objs[0:0]

		if c.deletedCount%10000 == 0 {
			fmt.Printf("Deleted %d objects\n", c.deletedCount)
		}
	}
}

func (c *BucketCleaner) deleteObj(objChannel chan s3.ObjectIdentifier, bucketName string) {
	for obj := range objChannel {
		deleteObjectInput := &s3.DeleteObjectInput{
			Bucket:    aws.String(bucketName),
			Key:       obj.Key,
			VersionId: obj.VersionId,
		}

		_, err := c.svc.DeleteObject(deleteObjectInput)
		if err != nil {
			fmt.Printf("Failed to delete object. %v\n", err)
		} else {
			//fmt.Printf("deleted object: %v\n", obj)
			atomic.AddUint64(&c.deletedCount, 1)
		}

		if c.deletedCount%10000 == 0 {
			fmt.Printf("Deleted %d objects\n", c.deletedCount)
		}
	}
}

func (c *BucketCleaner) abortAllMultiparts(bucketName string) {
	var maxUploads int64
	maxUploads = 1000
	input := &s3.ListMultipartUploadsInput{
		Bucket:     aws.String(bucketName),
		MaxUploads: &maxUploads,
	}

	allUploads, err := c.svc.ListMultipartUploads(input)
	if err != nil {
		fmt.Printf("fail to list multipart uploads. %v\n", err)
		return
	}

	for {
		if len(allUploads.Uploads) > 0 {
			c.listedCount += uint64(len(allUploads.Uploads))
			fmt.Printf("got %d objects of bucket.\n", c.listedCount)
			for _, upload := range allUploads.Uploads {
				abortInput := &s3.AbortMultipartUploadInput{
					Bucket:   &bucketName,
					Key:      upload.Key,
					UploadId: upload.UploadId,
				}
				_, err = c.svc.AbortMultipartUpload(abortInput)
				if err != nil {
					fmt.Printf("fail to AbortMultipartUpload. %v\n", err)
				}
			}

			input.KeyMarker = allUploads.NextKeyMarker
			allUploads, err = c.svc.ListMultipartUploads(input)
			if err != nil {
				fmt.Printf("fail to list multipart uploads. %v\n", err)
			}
		} else {
			break
		}
	}
}

func (c *BucketCleaner) listObjs(objChannel chan s3.ObjectIdentifier, bucketName string) {
	fmt.Printf("listing buckets %v\n", bucketName)
	input := &s3.ListObjectVersionsInput{
		Bucket:    aws.String(bucketName),
		KeyMarker: aws.String(""),
	}
	output, err := c.svc.ListObjectVersions(input)
	if err != nil {
		fmt.Printf("fail to list object versions of bucket. %v\n", err)
	}

	for {
		if len(output.Versions) > 0 {
			c.listedCount += uint64(len(output.Versions))
			fmt.Printf("got %d objects of bucket.\n", c.listedCount)
			for _, object := range output.Versions {
				objChannel <- s3.ObjectIdentifier{
					Key:       object.Key,
					VersionId: object.VersionId,
				}
			}

			input.KeyMarker = output.NextKeyMarker
			output, err = c.svc.ListObjectVersions(input)
			if err != nil {
				fmt.Printf("fail to list objects of bucket. %v\n", err)
			}
		} else {
			break
		}
	}

	close(objChannel)
}
