module github.com/s4na/fts-go-s3/experiments/sdkbench

go 1.25.0

require (
	github.com/aws/aws-sdk-go-v2 v1.47.5
	github.com/aws/aws-sdk-go-v2/service/s3 v1.114.6
	github.com/aws/smithy-go v1.28.7
	github.com/s4na/fts-go-s3 v0.0.0
)

replace github.com/s4na/fts-go-s3 => ../..
