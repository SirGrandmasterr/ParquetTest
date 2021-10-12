package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	parquets3 "github.com/xitongsys/parquet-go-source/s3"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

type customerICE struct {
	CustomerID         *int64  `parquet:"name=customer_id, type=INT64, repetitiontype=OPTIONAL"`
	Compensationfactor *int32  `parquet:"name=compensation_rate, type=INT32, convertedtype=DECIMAL, scale=6, precision=8, repetitiontype=OPTIONAL"`
	Tier               *string `parquet:"name=tier, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	Allocation         *string `parquet:"name=allocation, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
}

type CompensationConverted struct {
	CustomerID string
	Multiplier float64
}

const (
	numRows = 20
)

func main() {

	// s3Example provides a sample write and read using the S3 Parquet File
	ctx := context.Background()
	bucket := "mesh-pa-consumer"
	key := "test/foobar.parquet"
	num := 100

	manager := SetupForTesting()

	// create new S3 file writer
	fw, err := parquets3.NewS3FileWriterWithClient(ctx, manager.client, bucket, key, "bucket-owner-full-control", nil)
	if err != nil {
		log.Println("Can't open file", err)
		return
	}
	// create new parquet file writer
	pw, err := writer.NewParquetWriter(fw, new(customerICE), 4)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}
	// write 100 student records to the parquet file
	for i := 0; i < num; i++ {
		Allocation := "Irrelevant"
		Tier := "Irrelevant"
		stu := customerICE{
			CustomerID:         create64(1234),
			Compensationfactor: create32(00100000),
			Allocation:         &Allocation,
			Tier:               &Tier,
		}
		if err = pw.Write(stu); err != nil {
			log.Println("Write error", err)
		}
	}
	// write parquet file footer
	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop err", err)
	}

	err = fw.Close()
	if err != nil {
		log.Println("Error closing S3 file writer")
		log.Println(err)
	}
	log.Println("Write Finished")

	pw.WriteStop()
	fw.Close()

	Manager := SetupForTesting()

	testreader, err := parquets3.NewS3FileReaderWithClient(ctx, Manager.client, "mesh-pa-consumer", "test/foobar.parquet")
	if err != nil {
		log.Println(err)
	}
	fmt.Println("Created Reader, trying to download something")
	prs3, err := reader.NewParquetReader(testreader, new(customerICE), 4)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}
	num = int(prs3.GetNumRows())
	converted := make([]CompensationConverted, 0)
	for i := 0; i < num/10; i++ {
		structArray := make([]customerICE, 10) //read 10 rows
		if err = prs3.Read(&structArray); err != nil {
			log.Println("Read error", err)
		}
		converted = append(converted, CustomerICEToCompensationConverted(structArray)...)
		//log.Println(stus)
	}
	numRemaining := num % (num / numRows)
	structArray := make([]customerICE, numRemaining) //read 10 rows
	if err = prs3.Read(&structArray); err != nil {
		fmt.Print(err)
	}
	converted = append(converted, CustomerICEToCompensationConverted(structArray)...)
	fmt.Print(converted)
	prs3.ReadStop()
}

func create64(x int64) *int64 {
	return &x
}

func create32(x int32) *int32 {
	return &x
}

type S3Manager struct {
	client *s3.S3
}

func SetupForTesting() *S3Manager {
	awsConf := &aws.Config{
		Credentials:      credentials.NewStaticCredentials("minio_access_key", "minio_secret_key", ""),
		Endpoint:         aws.String("http://127.0.0.1:9000"),
		Region:           aws.String("eu-west-1"),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	}

	client := s3.New(session.Must(session.NewSession(awsConf)))

	_, err := client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String("mesh-pa-consumer"),
	})
	if err != nil {
		er := err.(awserr.Error)
		if er.Code() != s3.ErrCodeBucketAlreadyExists && er.Code() != s3.ErrCodeBucketAlreadyOwnedByYou {
			panic(err)
		}
	}

	return &S3Manager{
		client: s3.New(session.Must(session.NewSession(awsConf))),
	}
}

func DECIMAL_INT_ToString(dec int32, precision int, scale int) string {
	s := int(math.Pow10(scale))
	integer, fraction := int(dec)/s, int(dec)%s
	ans := strconv.Itoa(integer)
	if scale > 0 {
		ans += "." + strconv.Itoa(fraction)
	}
	return ans
}

func IntToFloat(toConvert int32) float64 {
	result := float64(toConvert) / float64(math.Pow10(6))
	return result
}

func CustomerICEToCompensationConverted(ICEs []customerICE) []CompensationConverted {
	converted := make([]CompensationConverted, len(ICEs))
	for index, data := range ICEs {
		conv := CompensationConverted{
			CustomerID: strconv.Itoa(int(*data.CustomerID)),
			Multiplier: intToFloat(*data.Compensationfactor),
		}
		converted[index] = conv
	}
	return converted
}

// Converts the int32 of given parquetfile into the float64 that we save
func intToFloat(toConvert int32) float64 {
	result := float64(toConvert) / float64(math.Pow10(6))
	return result
}
