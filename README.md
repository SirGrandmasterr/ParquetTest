# ParquetTest
Proof of Concept on how to read parquet from S3


What to do: 
- docker-compose up
- go to localhost:9000
- create a bucket called "icescore"
- upload the flat.parquet from output folder
- start main.go
- it should create the flat.parquet file again, print it's result from disk and again from S3
