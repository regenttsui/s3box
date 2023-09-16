package s3box

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// TODO modify print of result and error
type BucketCleaner struct {
	svc          *s3.S3
	deletedCount uint64
}

func NewBucketCleaner(svc *s3.S3) *BucketCleaner {
	c := &BucketCleaner{
		svc:          svc,
		deletedCount: 0,
	}
	return c
}

// EmptyBucket is to empty objects in the bucket concurrently.
// objChanCap is the capacity of channel to store the objects from listObjs
// multiDel decides whether to delete multiple objects in a request
func (c *BucketCleaner) EmptyBucket(bucketName string, deleteWorkerNum, objChanCap int, multiDel, deleteBucket bool) error {
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
		if multiDel {
			go func() {
				defer wg.Done()
				c.deleteObjs(objChannel, bucketName)
			}()
		} else {
			go func() {
				defer wg.Done()
				c.deleteObj(objChannel, bucketName)
			}()
		}
	}

	wg.Wait()
	fmt.Println("all task completed")

	if deleteBucket {
		deleteBucketInput := &s3.DeleteBucketInput{
			Bucket: aws.String(bucketName),
		}
		_, err := c.svc.DeleteBucket(deleteBucketInput)
		if err != nil {
			fmt.Printf("delete bucket err:%s\n", err)
			return err
		}
		fmt.Printf("deleted bucket:%s\n", bucketName)
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
		bktName := *b.Name
		if containedStr == "" || strings.Contains(bktName, containedStr) {
			c.EmptyBucket(bktName, 3, 1000, true, true)
		}
	}
	return nil
}

func (c *BucketCleaner) crontabPrintResults(startTime time.Time) {
	timeTicker := time.NewTicker(time.Duration(1) * time.Second)
	defer timeTicker.Stop()
	tps := 0.0
	for {
		<-timeTicker.C
		timeDiff := time.Since(startTime)
		tps = float64(c.deletedCount) / timeDiff.Seconds()
		fmt.Printf("TPS:%f\n", tps)
		if c.deletedCount > 0 && c.deletedCount%10000 == 0 {
			fmt.Printf("deleted %d objects\n", c.deletedCount)
		}
	}
}

func (c *BucketCleaner) deleteObjs(objChannel chan s3.ObjectIdentifier, bucketName string) {
	maxDelNum := 1000
	objs := make([]*s3.ObjectIdentifier, 0, maxDelNum)
	for {
		select {
		case obj, ok := <-objChannel:
			if !ok {
				// If there are any undeleted objects, delete them first and then exit
				if len(objs) > 0 {
					c.doDeleteObjsReq(objs, bucketName)
				}
				return
			}
			objId := s3.ObjectIdentifier{
				Key:       obj.Key,
				VersionId: obj.VersionId,
			}
			objs = append(objs, &objId)
			if len(objs) < maxDelNum {
				continue
			}

			c.doDeleteObjsReq(objs, bucketName)
			objs = objs[0:0]

		default:
			// If no data is available in the channel, delete the objects directly and continue the next loop
			if len(objs) > 0 {
				c.doDeleteObjsReq(objs, bucketName)
				objs = objs[0:0]
			}
		}
	}
}

func (c *BucketCleaner) doDeleteObjsReq(objs []*s3.ObjectIdentifier, bucketName string) {
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
		atomic.AddUint64(&c.deletedCount, 1)
	}
}

func (c *BucketCleaner) deleteObj(objChannel chan s3.ObjectIdentifier, bucketName string) {
	for {
		select {
		case obj, ok := <-objChannel:
			if !ok {
				return
			}
			deleteObjectInput := &s3.DeleteObjectInput{
				Bucket:    aws.String(bucketName),
				Key:       obj.Key,
				VersionId: obj.VersionId,
			}

			_, err := c.svc.DeleteObject(deleteObjectInput)
			if err != nil {
				fmt.Printf("Failed to delete object. %v\n", err)
			} else {
				atomic.AddUint64(&c.deletedCount, 1)
			}
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

	listedCount := 0
	for {
		if len(allUploads.Uploads) > 0 {
			listedCount += len(allUploads.Uploads)
			fmt.Printf("got %d objects of bucket %v\n", listedCount, bucketName)
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
	defer close(objChannel)
	input := &s3.ListObjectVersionsInput{
		Bucket: aws.String(bucketName),
		//KeyMarker: aws.String(""),
	}
	output, err := c.svc.ListObjectVersions(input)
	if err != nil {
		fmt.Printf("fail to list object versions of bucket. %v\n", err)
		return
	}

	listedCount := 0
	for {
		if len(output.Versions) > 0 {
			listedCount += len(output.Versions)
			fmt.Printf("got %d objects of bucket %v\n", listedCount, bucketName)
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
		} else if len(output.DeleteMarkers) > 0 {
			listedCount += len(output.DeleteMarkers)
			fmt.Printf("got %d objects of bucket %v\n", listedCount, bucketName)
			for _, object := range output.DeleteMarkers {
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
}
