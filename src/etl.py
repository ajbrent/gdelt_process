import requests
import datetime
import random
import io
import re
import time
import zipfile

import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

import logging
import boto3
import botocore

logger = logging.getLogger()

TXT_LINK = 'http://data.gdeltproject.org/gdeltv2/lastupdate.txt'

GKG_HEADERS = [
    'GKG_RECORD_ID',
    'DATE',
    'V2SOURCECOLLECTIONIDENTIFIER',
    'V2SOURCECOMMONNAME',
    'V2DOCUMENTIDENTIFIER',
    'V1COUNTS',
    'V2.1COUNTS',
    'V1THEMES',
    'V2ENHANCEDTHEMES',
    'V1LOCATIONS',
    'V2ENHANCEDLOCATIONS',
    'V1PERSONS',
    'V2ENHANCEDPERSONS',
    'V1ORGANIZATIONS',
    'V2ENHANCEDORGANIZATIONS',
    'V1.5TONE',
    'V2.1ENHANCEDDATES',
    'V2GCAM',
    'V2.1SHARINGIMAGE',
    'V2.1RELATEDIMAGES',
    'V2.1SOCIALIMAGEEMBEDS',
    'V2.1SOCIALVIDEOEMBEDS',
    'V2.1QUOTATIONS',
    'V2.1ALLNAMES',
    'V2.1AMOUNTS',
    'V2.1TRANSLATIONINFO',
    'V2EXTRASXML'
]

GKG_DICT = {
    0: str, # GKGRECORDID
    2: int, # V2SOURCECOLLECTIONIDENTIFIER
    3: str, # V2SOURCECOMMONNAME
    4: str, # V2DOCUMENTIDENTIFIER
    5: str, # V1COUNTS
    6: str, # V2.1COUNTS
    7: str, # V1THEMES
    8: str, # V2ENHANCEDTHEMES
    9: str, # V1LOCATIONS
    10: str, # V2ENHANCEDLOCATIONS
    11: str, # V1PERSONS
    12: str, # V2ENHANCEDPERSONS
    13: str, # V1ORGANIZATIONS
    14: str, # V2ENHANCEDORGANIZATIONS
    15: str, # V1.5TONE
    16: str, # V2.1ENHANCEDDATES
    17: str, # V2GCAM
    18: str, # V2.1SHARINGIMAGE
    19: str, # V2.1RELATEDIMAGES
    20: str, # V2.1SOCIALIMAGEEMBEDS
    21: str, # V2.1SOCIALVIDEOEMBEDS
    22: str, # V2.1QUOTATIONS
    23: str, # V2.1ALLNAMES
    24: str, # V2.1AMOUNTS
    25: str, # V2.1TRANSLATIONINFO
    26: str, # V2EXTRASXML
}

def check_links(link_dict: dict[str, str]) -> str:
    """Returns true if and only if all links are valid."""
    if len(link_dict) != 3:
        return None
    dt_str = None
    for key, link in link_dict.items():
        pattern_1 = r'http://data\.gdeltproject\.org/gdeltv2/\d{14}\.'
        pattern_2 = r'\.\D{3}\.zip'
        pattern = pattern_1 + re.escape(key) + pattern_2
        if not re.match(pattern, link):
            return None
        curr_dt_str = re.compile(r'\b\d{14}\b').findall(link)[0]
        if dt_str is None:
            dt_str = curr_dt_str
        elif curr_dt_str != dt_str:
            return None
    return curr_dt_str

def get_zip_links(t: int = 1, prev_stamp: int = None) -> tuple[int, dict[str, str]]:
    # get and unzip csv file every 15 minutes
    if t > 64:
        raise ValueError("Exceeded maximum number of retries")
    if t == 1:
        client = boto3.client('dynamodb')
        date_response = client.scan(
            TableName='LatestTime'
        )
        items = date_response['Items']
        if len(items) == 1: 
            prev_stamp = items[0]['LatestTime']['N']
        elif len(items) > 1:
            raise ValueError("LatestTime should be single item table.")

    response = None
    try:
        response = requests.get(TXT_LINK)
        response.raise_for_status()
    except requests.HTTPError as e:
        print("Error: ", e)
        raise e
    rows = response.text.split('\n')[:3]
    link_dict = {}
    for row in rows:
        if len(row.split()) == 0:
            continue
        link = row.split()[-1]
        if '.gkg.' in link:
           link_dict['gkg'] = link
        elif '.export.' in link:
            link_dict['export'] = link
        elif '.mentions.' in link:
            link_dict['mentions'] = link
        else:
            raise ValueError("Invalid link")

    curr_dt = check_links(link_dict)
    curr_stamp = None
    if curr_dt is None:
        raise ValueError("Invalid links")
    else:
        curr_stamp = int(datetime.datetime.strptime(curr_dt, '%Y%m%d%H%M%S').timestamp())
    if prev_stamp is not None:
        logger.info(f'Current timestamp: {curr_stamp}')
        logger.info(f'Previous timestamp: {prev_stamp}')
        if curr_stamp == prev_stamp:
            logger.warning(f'No new data available. Retrying in {t} seconds.')
            offset = float(random.randint(0, 1000)) / 1000
            time.sleep(t + offset)
            return get_zip_links(t*2, prev_stamp)
        if int(curr_stamp) < int(prev_stamp):
            raise ValueError("previously inserted datetime is newer than current datetime")
        
    client = boto3.client('dynamodb')
    if prev_stamp is not None:
        _ = client.delete_item(TableName='LatestTime', Key={'LatestTime': {'N': str(prev_stamp)}})
    _ = client.put_item(TableName='LatestTime', Item={'LatestTime': {'N': str(curr_stamp)}})

    logger.info(f'successfully retrieved data for {curr_stamp}')
    return curr_dt, link_dict

def rm_subtopics(topics: list[str]) -> list[str]:
    """Remove subtopics from the list of topics."""
    # Could be a problem i.e. 'San Francisco' and 'Francisco' are two different topics
    indexes = []
    for i in range(len(topics)):
        if i in indexes:
            continue
        for j in range(i+1, len(topics)):
            if j in indexes:
                continue
            if topics[i] in topics[j]:
                indexes.append(j)
                break
            if topics[j] in topics[i]:
                indexes.append(i)
    filtered_topics = [topics[i] for i in range(len(topics)) if i not in indexes]
    return filtered_topics

def gkg_process(df: pd.DataFrame) -> pd.DataFrame:
    """Process the GKG file."""
    # Looking only at web documents: code 1
    df = df[df['V2SOURCECOLLECTIONIDENTIFIER'] == 1]
    used_cols = ['GKG_RECORD_ID', 'DATE', 'V2SOURCECOMMONNAME', 'V2DOCUMENTIDENTIFIER', 'V2ENHANCEDORGANIZATIONS', 'V2ENHANCEDPERSONS', 'V2.1ALLNAMES']
    df = df[used_cols]
    pattern = r'([^,;]+),'
    all_names = df['V2.1ALLNAMES'].astype(str).apply(lambda x: [] if x is None or x == '' else re.findall(pattern, x))
    orgs = df['V2ENHANCEDORGANIZATIONS'].astype(str).apply(lambda x: [] if x is None or x == '' else re.findall(pattern, x))
    persons = df['V2ENHANCEDPERSONS'].astype(str).apply(lambda x: [] if x is None or x == '' else re.findall(pattern, x))
    df['TOPICS'] = all_names + orgs + persons
    df['TOPICS'] = df['TOPICS'].apply(lambda x: list(set(x)))
    df = df[['V2SOURCECOMMONNAME', 'V2DOCUMENTIDENTIFIER', 'TOPICS']]
    df['TOPICS'] = df['TOPICS'].apply(rm_subtopics)
    
    topic_dict = {}
    for _, row in df.iterrows():
        src = row['V2SOURCECOMMONNAME']
        url = row['V2DOCUMENTIDENTIFIER']
        for topic in row['TOPICS']:
            if topic in topic_dict:
                topic_dict[topic][0].append(src)
                topic_dict[topic][1].append(url)
            else:
                topic_dict[topic] = [[src], [url]]
    topic_df = pd.DataFrame([(k, *v) for k, v in topic_dict.items()], columns=['topics', 'sources', 'urls'])
    topic_df['counts'] = topic_df['urls'].apply(len)
    topic_df['src_counts'] = topic_df['sources'].apply(lambda x: len(set(x)))
    topic_df = topic_df[topic_df['topics'] != 'Associated Press']
    return topic_df

def create_df(link: str) -> pd.DataFrame:
    """Download, create dataframe and return data for DynamoDB model."""
    zip_response = None
    try:
        zip_response = requests.get(link)
        zip_response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(e)
        return None
    with zipfile.ZipFile(io.BytesIO(zip_response.content)) as z:
        csv_file_name = z.namelist()[0]
        with z.open(csv_file_name) as f:
            out_df = {}
            if '.gkg.' in link:
                gkg_df = pd.read_csv(f, sep='\t', parse_dates=[1], date_format='%Y%m%d%H%M%S', dtype=GKG_DICT, names=GKG_HEADERS, encoding='latin-1')
                out_df = gkg_process(gkg_df)

    return out_df

def to_s3(df: pd.DataFrame, bucket: str, dt: str, name: str) -> bool:
    buffer = io.BytesIO()
    table = pa.Table.from_pandas(df)
    pq.write_table(table, buffer)

    buffer.seek(0)

    client = boto3.client('s3')
    file_name = dt + '-' + name + '.parquet'

    try:
        _ = client.put_object(Bucket=bucket, Key=file_name, Body=buffer.getvalue())
    except botocore.exceptions.ClientError as error:
        raise error
    except botocore.exceptions.ParamValidationError as error:
        raise ValueError('The parameters you provided are incorrect: {}'.format(error))
    return True

def score_func(row: pd.Series) -> float:
    """Calculate geometric mean logged for topic."""
    return np.log(row['counts']) + 2 * np.log(row['src_counts']) + 1

def update_scores(new_data: pd.DataFrame, scores_df: pd.DataFrame, bucket: str, old_df: pd.DataFrame) -> bool:
    """Update scores in S3."""
    client = boto3.client('s3')

    merge_df = new_data
    if scores_df is not None:
        merge_df = pd.merge(scores_df, new_data, on='topics', how='outer')
        merge_df.fillna(0, inplace=True)
    else:
        merge_df['day_counts'] = 0
        merge_df['day_sources'] = [[] for _ in range(len(merge_df))]
        merge_df['day_src_counts'] = 0
        merge_df['day_scores'] = 0

    merge_df['day_counts'] = merge_df['day_counts'] + merge_df['counts']
    merge_df['day_src_counts'] = merge_df['day_src_counts'] + merge_df['src_counts']
    merge_df = merge_df.drop(columns=['counts', 'src_counts'])

    if old_df is not None:
        merge_df = pd.merge(merge_df, old_df, on='topics', how='outer')
        merge_df.fillna(0, inplace=True)
        merge_df = merge_df.drop(columns=['urls'])
    else:
        merge_df['sources'] = [[] for _ in range(len(merge_df))]
        merge_df['counts'] = 0
        merge_df['src_counts'] = 0
    # old source counts are problematic
    merge_df['day_counts'] = merge_df['day_counts'] - merge_df['counts']
    merge_df['day_sources'] = merge_df['sources'] + merge_df['day_sources']
    merge_df['day_sources'] = merge_df['day_sources'].apply(lambda x: list(set(x)))
    merge_df['day_src_counts'] = len(merge_df['day_sources'])

    merge_df = merge_df[merge_df['day_counts'] > 0]
    merge_df['day_scores'] = np.log(merge_df['day_counts']) + 2 * np.log(merge_df['day_src_counts']) + 1
    merge_df = merge_df.drop(columns=['sources', 'counts', 'src_counts'])
    
    _ = client.put_object(Bucket=bucket, Key='day-scores.parquet', Body=merge_df.to_parquet())
    return True

def upload(topic_df: pd.DataFrame, bucket: str, dt: str) -> bool:
    """Upload dataframe to S3."""
    df_put = to_s3(topic_df, bucket, dt, 'scores')
    return df_put
