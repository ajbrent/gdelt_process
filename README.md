# Lambda Function to Collect GDELT Data

This is an AWS Lambda Function, created via the [AWS base image](https://docs.aws.amazon.com/lambda/latest/dg/python-image.html#python-image-base), that takes the GDELT v2.0 CSVs, and uses Pandas to extract parquet files containing the sources, URLs, and topics and place them into an [S3 Bucket](https://aws.amazon.com/s3/).

Per GDELT, new csv files are updated every 15 minutes, so this function will be run via the [EventBridge Scheduler](https://aws.amazon.com/eventbridge/scheduler/) every 15 minutes as well (exponential backoff in case of late update). At the end of each call the data is writtent to DynamoDB via a batch write.

Currently using only the Global Knowledge Graph (GKG) file. Documentation found [here](https://blog.gdeltproject.org/gdelt-2-0-our-global-world-in-realtime/)
