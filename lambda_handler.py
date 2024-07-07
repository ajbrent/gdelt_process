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
    topic_df, src_df = etl.create_dfs(zip_links['gkg'])

    scores_file = 'day-scores.parquet'
    srcs_file = 'day-srcs.parquet'
    scores_df = utils.df_from_bucket(S3_BUCKET, scores_file)
    srcs_df = utils.df_from_bucket(S3_BUCKET, srcs_file)

    dead_time = datetime.datetime.strptime(curr_dt, '%Y%m%d%H%M%S') - datetime.timedelta(days=1)
    old_file = dead_time.strftime('%Y%m%d%H%M%S') + '-scores.parquet'
    old_src_file = dead_time.strftime('%Y%m%d%H%M%S') + '-srcs.parquet'

    old_df = utils.df_from_bucket(S3_BUCKET, old_file)
    old_src_df = utils.df_from_bucket(S3_BUCKET, old_src_file)

    combine_df, combine_src_df = combine_topics.combine_df_topics(topic_df, src_df, old_df)
    logger.warning(combine_df.columns)
    scores_updated = etl.update_scores(combine_df, combine_src_df, scores_df, srcs_df, S3_BUCKET, old_df, old_src_df)
    logger.warning(combine_df.columns)
    success = etl.upload(combine_df, S3_BUCKET, curr_dt)
    if not (success and scores_updated):
        logger.error('Failed to write to S3.')
        raise Exception('Failed to write to S3.')
    
    logger.info(f'Data uploaded to S3.\n{event}')
