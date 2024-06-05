# Lambda Function to Collect GDELT Data

This is an AWS Lambda Function, created via the [AWS base image](https://docs.aws.amazon.com/lambda/latest/dg/python-image.html#python-image-base), that takes the GDELT v2.0 CSVs, and uses Pandas to extract parquet files containing the sources, URLs, and topics and place them into an [S3 Bucket](https://aws.amazon.com/s3/).

Currently, the function creates two new parquet files in an S3 bucket every 15 minutes. The filenames both start with the most recent 15 minute UTC timestamp formatted as 'YYYYmmDDHHMMSS'. There is then a '-scores' file which has columns 'topics' (proper noun parsed out from articles by GDELT), 'counts' (number of times given topic is mentioned), 'src_counts' (number of unique sources mentioning the topic), 'scores' (`ln(counts) + 2ln(src_counts) + 1`). The score is essentially a weighted geometric mean of source and article counts. The second file, suffixed with '-urls', has the same length and order of the '-scores' file with two columns of lists (matched along the indices) of the 'sources' and 'urls' respectively. The aggregate counts and scores of the past 24 hours are contained in the 'day-scores' file (added to each time a new file is added and subtracted from by every set that falls out of scope).

Per GDELT, new csv files are updated every 15 minutes, so this function will be run via the [EventBridge Scheduler](https://aws.amazon.com/eventbridge/scheduler/) every 15 minutes as well (exponential backoff in case of late update). At the end of each call the data is writtent to DynamoDB via a batch write.

Currently using only the Global Knowledge Graph (GKG) file. Documentation found [here](https://blog.gdeltproject.org/gdelt-2-0-our-global-world-in-realtime/)
