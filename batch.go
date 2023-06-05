package S3Box

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"sync"
	"sync/atomic"
	"time"
)

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

// EmptyBucket is to empty objects in the bucket concurrently
func (c *BucketCleaner) EmptyBucket(bucketName string, workerNum, objChanCap, multiDelNum int, versioned, deleteBucket bool) {
	var wg sync.WaitGroup
	wg.Add(workerNum + 1)
	startTime := time.Now()
	go c.crontabPrintResults(startTime)
	// objChannel存放实际的对象名
	objChannel := make(chan string, objChanCap)
	// listObjs并将对象名放入objChannel
	if versioned {
		//TODO list versions
	} else {
		go c.listObjsWorker(objChannel, bucketName)
	}

	// 并发删除对象
	for i := 0; i < workerNum; i++ {
		if multiDelNum > 0 {
			go func() {
				defer wg.Done()
				c.deleteObjsWorker(objChannel, bucketName, multiDelNum)
			}()
		} else {
			go func() {
				defer wg.Done()
				c.deleteObjWorker(objChannel, bucketName)
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
			fmt.Printf("fail to delete bucket. %v\n", err)
		}
	}

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

func (c *BucketCleaner) deleteObjsWorker(objChannel chan string, bucketName string, multiDelNum int) {
	objs := make([]*s3.ObjectIdentifier, 0, multiDelNum)
	for obj := range objChannel {
		objId := s3.ObjectIdentifier{
			Key: aws.String(obj),
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

func (c *BucketCleaner) deleteObjWorker(objChannel chan string, bucketName string) {
	for obj := range objChannel {
		deleteObjectInput := &s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(obj),
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

func (c *BucketCleaner) listObjsWorker(objChannel chan string, bucketName string) {
	fmt.Printf("listing buckets %v\n", bucketName)
	listObjectsInput := &s3.ListObjectsInput{
		Bucket: aws.String(bucketName),
		Marker: aws.String(""),
	}
	ListObjectsOutput, err := c.svc.ListObjects(listObjectsInput)
	if err != nil {
		fmt.Printf("fail to list objects of bucket. %v\n", err)
	}

	for {
		if len(ListObjectsOutput.Contents) > 0 {
			c.listedCount += uint64(len(ListObjectsOutput.Contents))
			fmt.Printf("got %d objects of bucket.\n", c.listedCount)
			for _, object := range ListObjectsOutput.Contents {
				objChannel <- *object.Key
			}
			ListObjectsOutput, err = c.svc.ListObjects(listObjectsInput)
			if err != nil {
				fmt.Printf("fail to list objects of bucket. %v\n", err)
			}
			listObjectsInput.Marker = ListObjectsOutput.NextMarker
		} else {
			break
		}
	}
	close(objChannel)
}