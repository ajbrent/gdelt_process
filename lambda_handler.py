from src import etl
import logging

logger = logging.getLogger()

def lambda_handler(event, context):
    curr_stamp, zip_links = etl.get_zip_links()
    topic_dict = etl.create_dict(zip_links['gkg'])
    success = etl.to_dynamodb(curr_stamp, topic_dict)
    if not success:
        logger.error('Failed to write to DynamoDB.')
        raise Exception('Failed to write to DynamoDB.')
    
    logger.info(f'Data uploaded to DynamoDB.\n{event}')
