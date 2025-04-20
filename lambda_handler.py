from src import etl
from src import combine_topics
from src import utils
import datetime
import logging
import sys

logger = logging.getLogger()
S3_BUCKET = 'gdelt-data-prod'

def lambda_handler(event, context):
    curr_dt, zip_links = etl.get_zip_links()
    new_base_table = etl.create_tables(zip_links['gkg'])

    entity_table = utils.table_from_bucket(S3_BUCKET, 'entities.parquet')
    src_table = utils.table_from_bucket(S3_BUCKET, 'sources.parquet')
