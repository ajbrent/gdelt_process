from src import etl
import logging
import os

logger = logging.getLogger()

def lambda_handler(event, context):
    curr_dt, zip_links = etl.get_zip_links()
    topic_df = etl.create_dict(zip_links['gkg'])
    success = etl.clean_and_upload(topic_df, os.getenv('S3_BUCKET'), curr_dt)
    if not success:
        logger.error('Failed to write to S3.')
        raise Exception('Failed to write to S3.')
    
    logger.info(f'Data uploaded to S3.\n{event}')
