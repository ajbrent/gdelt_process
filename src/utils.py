import io
import pandas as pd
import pyarrow.parquet as pq
import logging
import boto3
import botocore

logger = logging.getLogger(__name__)

def df_from_bucket(bucket: str, key: str) -> pd.DataFrame:
    """Read parquet file from S3 and return dataframe."""
    client = boto3.client('s3')
    response = None
    try:
        response = client.get_object(Bucket=bucket, Key=key)
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] != 'NoSuchKey':
            raise error
        else:
            logger.info('No previous scores found.')
            response = None
    except botocore.exceptions.ParamValidationError as error:
        raise ValueError('The parameters provided are incorrect: {}'.format(error))
    
    df = None
    if response is not None:
        buffer = io.BytesIO(response['Body'].read())
        table = pq.read_table(buffer)
        df = table.to_pandas()
    return df