from src import etl
from src import combine_topics
from src import utils
import datetime
import logging
import os

logger = logging.getLogger()
S3_BUCKET = 'gdelt-data-prod'

def lambda_handler(event, context):
    curr_dt, zip_links = etl.get_zip_links()
    topic_df = etl.create_df(zip_links['gkg'])

    scores_file = 'day-scores.parquet'
    scores_df = etl.create_scores_df(S3_BUCKET, scores_file)

    dead_time = datetime.datetime.strptime(curr_dt, '%Y%m%d%H%M%S') - datetime.timedelta(days=1)
    old_file = dead_time.strftime('%Y%m%d%H%M%S') + '-scores.parquet'
    old_df = utils.df_from_bucket(S3_BUCKET, old_file)
    combine_df = combine_topics.combine_df_topics(topic_df, old_df)
    scores_updated = etl.update_scores(combine_df, scores_df, S3_BUCKET, old_df)
    success = etl.upload(combine_df, S3_BUCKET, curr_dt)
    if not (success and scores_updated):
        logger.error('Failed to write to S3.')
        raise Exception('Failed to write to S3.')
    
    logger.info(f'Data uploaded to S3.\n{event}')
