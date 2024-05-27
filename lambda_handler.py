from src import etl
import logging
import os

logger = logging.getLogger()
S3_BUCKET = 'gdelt-data-prod'

def lambda_handler(event, context):
    curr_dt, zip_links = etl.get_zip_links()
    topic_df = etl.create_df(zip_links['gkg'])
    success = etl.upload(topic_df, S3_BUCKET, curr_dt)
    if not success:
        logger.error('Failed to write to S3.')
        raise Exception('Failed to write to S3.')
    
    logger.info(f'Data uploaded to S3.\n{event}')
