from src import etl
from src import combine_topics
from src import utils
import datetime
import logging
import sys

logger = logging.getLogger()
S3_BUCKET = 'gdelt-data-prod'

def lambda_handler(event, context):
    sys.setrecursionlimit(10000)
    curr_dt, zip_links = etl.get_zip_links()
    url_df, topic_src_df= etl.create_dfs(zip_links['gkg'])

    k_topic_src_file = 'k-topic-src.parquet'
    k_topic_src_df = utils.df_from_bucket(S3_BUCKET, k_topic_src_file)

    dead_time = datetime.datetime.strptime(curr_dt, '%Y%m%d%H%M%S') - datetime.timedelta(days=1)
    old_topic_src_file = dead_time.strftime('%Y%m%d%H%M%S') + 'topic-src.parquet'
    old_topic_src_df = utils.df_from_bucket(S3_BUCKET, old_topic_src_file)

    url_df, topic_src_df = combine_topics.combine_df_topics(url_df, topic_src_df, old_topic_src_df)

    url_success = etl.upload(url_df, S3_BUCKET, curr_dt+'-url')
    topic_src_success = etl.upload(topic_src_df, S3_BUCKET, curr_dt+'-topic-src')
    if not (url_success and topic_src_success):
        logger.error(f'Failed to write {curr_dt} to S3.')
        raise Exception(f'Failed to write {curr_dt} to S3.')

    k_score_df, k_topic_src_df = etl.update_scores(topic_src_df, k_topic_src_df, old_topic_src_df)

    k_topic_src_success = etl.upload(k_topic_src_df, S3_BUCKET, 'k-topic-src')
    k_scores_success = etl.upload(k_score_df, S3_BUCKET, 'k-scores')
    if not (k_scores_success and k_topic_src_success):
        logger.error('Failed to write k to S3.')
        raise Exception('Failed to write k to S3.')
    
    logger.info(f'Data uploaded to S3.\n{event}')
