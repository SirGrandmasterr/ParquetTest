package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/SirGrandmasterr/ParquetTest/pkg/parquets3"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type customerICE struct {
	CustomerID               string  `parquet:"name=customer_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Country                  string  `parquet:"name=country, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CompensationPctOfDefault float32 `parquet:"name=compensation_pct_of_default, type=FLOAT"`
	Factor                   float32 `parquet:"name=factor, type=FLOAT"`
}

func main() {
	ctx := context.TODO()
	var err error
	w, err := os.Create("output/flat.parquet")
	if err != nil {
		log.Println("Can't create local file", err)
		return
	}
	//write
	pw, err := writer.NewParquetWriterFromWriter(w, new(customerICE), 4)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}
	pw.RowGroupSize = 128 * 1024 * 1024 //128M
	pw.CompressionType = parquet.CompressionCodec_SNAPPY
	num := 100
	for i := 0; i < num; i++ {
		stu := customerICE{
			CustomerID:               "customerID",
			Country:                  "us",
			CompensationPctOfDefault: float32(divideByThousand(i)),
			Factor:                   float32(divideByThousand(i)),
		}
		if err = pw.Write(stu); err != nil {
			log.Println("Write error", err)
		}
	}
	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
		return
	}
	log.Println("Write Finished")
	w.Close()
	///read
	fr, err := local.NewLocalFileReader("output/flat.parquet")
	if err != nil {
		log.Println("Can't open file")
		return
	}
	pr, err := reader.NewParquetReader(fr, new(customerICE), 3)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}
	num = int(pr.GetNumRows())
	for i := 0; i < num/10; i++ {
		if i%2 == 0 {
			pr.SkipRows(10) //skip 10 rows
			continue
		}
		stus := make([]customerICE, 10) //read 10 rows
		if err = pr.Read(&stus); err != nil {
			log.Println("Read error", err)
		}
		log.Println(stus)
	}
	pr.ReadStop()
	fr.Close()

	Manager := SetupForTesting()
	testreader, err := parquets3.NewS3FileReaderWithClient(ctx, Manager.client, "icescore", "flat.parquet")
	fmt.Println("Created Reader, trying to download something")
	prs3, err := reader.NewParquetReader(testreader, new(customerICE), 4)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}
	num = int(prs3.GetNumRows())
	for i := 0; i < num/10; i++ {
		if i%2 == 0 {
			prs3.SkipRows(10) //skip 10 rows
			continue
		}
		stus := make([]customerICE, 10) //read 10 rows
		if err = prs3.Read(&stus); err != nil {
			log.Println("Read error", err)
		}
		log.Println(stus)
	}
	prs3.ReadStop()
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

	return &S3Manager{
		client: s3.New(session.Must(session.NewSession(awsConf))),
	}
}

func divideByThousand(num int) float32 {
	return float32(num) / 1000.0
}
