# Lambda Function to Collect GDELT Data

This is an AWS Lambda Function, created via the [AWS base image](https://docs.aws.amazon.com/lambda/latest/dg/python-image.html#python-image-base), that takes the GDELT v2.0 CSVs, and uses Pandas to process them into an [AWS DynamoDB](https://aws.amazon.com/dynamodb/) table:
```
NewsArticles:
    Partition Key: datetime(N) 
    Sort Key: topic(S)
```
Where datetime is in the format YYYYMMDDHHmmSS, and topic is a proper noun pulled from GDELT data (i.e. 'Barack Obama', or 'China'). This function then adds a list of the URLs that apply to each datetime, topic, and source as an attribute of each object inserted. Per GDELT, new csv files are updated every 15 minutes, so this function will be run via the [EventBridge Scheduler](https://aws.amazon.com/eventbridge/scheduler/) every 15 minutes as well (exponential backoff in case of late update). At the end of each call the data is writtent to DynamoDB via a batch write.

Currently processing only the Global Knowledge Graph (GKG) file. Documentation found [here](https://blog.gdeltproject.org/gdelt-2-0-our-global-world-in-realtime/)
