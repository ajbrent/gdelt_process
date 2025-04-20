import requests
import datetime
import random
import io
import re
import time
import zipfile
import duckdb

import pandas as pd

import pyarrow.csv as pv
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import logging
import boto3
import botocore

from pyiceberg import catalog

logger = logging.getLogger()

TXT_LINK = 'http://data.gdeltproject.org/gdeltv2/lastupdate.txt'

GKG_HEADERS = [
    'GKG_RECORD_ID',
    'GKG_DATE',
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

TABLE_LIST = [
    'articles',
    'counts',
    'themes',
    'locations',
    'persons',
    'organizations',
    'dates',
    'gcam',
    'related_images',
    'social_image_embeds',
    'social_video_embeds',
    'quotations',
    'all_names',
    'amounts',
    'translation_info'
]

USED_GKG_HEADERS = [
    'GKG_RECORD_ID',
    'GKG_DATE',
    'V2SOURCECOLLECTIONIDENTIFIER',
    'V2SOURCECOMMONNAME',
    'V2DOCUMENTIDENTIFIER',
    'V2.1COUNTS',
    'V2ENHANCEDTHEMES',
    'V2ENHANCEDLOCATIONS',
    'V2ENHANCEDPERSONS',
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

BASE_COLS = {
    'articles': [
        ('GKG_RECORD_ID', 'record_id'),
        ("GKG_DATE", 'collection_timestamp'),
        ('V2SOURCECOLLECTIONIDENTIFIER', 'src_collection_identifier'),
        ('V2SOURCECOMMONNAME', 'src_common_name'),
        ('V2DOCUMENTIDENTIFIER', 'doc_identifier'),
    ],
    'counts': [
        ('GKG_RECORD_ID', 'record_id')
    ],
    'themes': [
        ('GKG_RECORD_ID', 'record_id')
    ],
    'locations': [
        ('GKG_RECORD_ID', 'record_id')
    ],
    'persons': [
        ('GKG_RECORD_ID', 'record_id')
    ],
    'organizations': [
        ('GKG_RECORD_ID', 'record_id')
    ],
    'dates': [
        ('GKG_RECORD_ID', 'record_id')
    ],
    'gcam': [
        ('GKG_RECORD_ID', 'record_id'),
        ('V2GCAM', 'gcam_str')
    ],
    'related_images': [
        ('GKG_RECORD_ID', 'record_id')
    ],
    'sharing_image': [
        ('GKG_RECORD_ID', 'record_id'),
        ('V2.1SHARINGIMAGE', 'url')
    ],
    'social_image_embeds': [
        ('GKG_RECORD_ID', 'record_id')
    ],
    'social_video_embeds': [
        ('GKG_RECORD_ID', 'record_id')
    ],
    'quotations': [
        ('GKG_RECORD_ID', 'record_id')
    ],
    'all_names': [
        ('GKG_RECORD_ID', 'record_id')
    ],
    'amounts': [
        ('GKG_RECORD_ID', 'record_id')
    ],
    'translation_info': [
        ('GKG_RECORD_ID', 'record_id')
    ],
    'xml': [
        ('GKG_RECORD_ID', 'record_id'),
        ('V2EXTRASXML', 'xml')
    ]
}

GKG_DICT = {
    'GKGRECORDID': pa.string(), # GKGRECORDID
    'GKG_DATE': pa.timestamp(unit='s'), # GKG_DATE
    'V2SOURCECOLLECTIONIDENTIFIER': pa.uint16(), # V2SOURCECOLLECTIONIDENTIFIER
    'V2SOURCECOMMONNAME': pa.string(), # V2SOURCECOMMONNAME
    'V2DOCUMENTIDENTIFIER': pa.string(), # V2DOCUMENTIDENTIFIER
    'V1COUNTS': pa.string(), # V1COUNTS
    'V2.1COUNTS': pa.string(), # V2.1COUNTS
    'V1THEMES': pa.string(), # V1THEMES
    'V2ENHANCEDTHEMES': pa.string(), # V2ENHANCEDTHEMES
    'V1LOCATIONS': pa.string(), # V1LOCATIONS
    'V2ENHANCEDLOCATIONS': pa.string(), # V2ENHANCEDLOCATIONS
    'V1PERSONS': pa.string(), # V1PERSONS
    'V2ENHANCEDPERSONS': pa.string(), # V2ENHANCEDPERSONS
    'V1ORGANIZATIONS': pa.string(), # V1ORGANIZATIONS
    'V2ENHANCEDORGANIZATIONS': pa.string(), # V2ENHANCEDORGANIZATIONS
    'V1.5TONE': pa.string(), # V1.5TONE
    'V2.1ENHANCEDDATES': pa.string(), # V2.1ENHANCEDDATES
    'V2GCAM': pa.string(), # V2GCAM
    'V2.1SHARINGIMAGE': pa.string(), # V2.1SHARINGIMAGE
    'V2.1RELATEDIMAGES': pa.string(), # V2.1RELATEDIMAGES
    'V2.1SOCIALIMAGEEMBEDS': pa.string(), # V2.1SOCIALIMAGEEMBEDS
    'V2.1SOCIALVIDEOEMBEDS': pa.string(), # V2.1SOCIALVIDEOEMBEDS
    'V2.1QUOTATIONS': pa.string(), # V2.1QUOTATIONS
    'V2.1ALLNAMES': pa.string(), # V2.1ALLNAMES
    'V2.1AMOUNTS': pa.string(), # V2.1AMOUNTS
    'V2.1TRANSLATIONINFO': pa.string(), # V2.1TRANSLATIONINFO
    'V2EXTRASXML': pa.string(), # V2EXTRASXML
}

STR_REGEX_DICT = {
    'counts': [{
        'base_col': 'V2.1COUNTS',
        'regexp': r"([^#;]+(?:#[^#;]+)*);?",
        'cols': [
            {'name': 'count_type', 'type': "STRING"},
            {'name': 'count', 'type': "INT64"},
            {'name': 'object_type', 'type': "STRING"},
            {'name': 'location_type', 'type': "INT64"},
            {'name': 'location_full_name', 'type': "STRING"},
            {'name': 'location_country_code', 'type': "STRING"},
            {'name': 'location_adm1_code', 'type': "STRING"},
            {'name': 'location_latitude', 'type': "DOUBLE"},
            {'name': 'location_longitude', 'type': "DOUBLE"},
            {'name': 'location_feature_id', 'type': "STRING"},
            {'name': 'char_offset', 'type': "INT64"}
        ]
    }],
    'themes': [{
        'base_col': 'V2ENHANCEDTHEMES',
        'regexp': r'([^,;]+(?:,[^,;]+)*);?',
        'cols': [
            'gkg_theme',
            'char_offset'
        ]
    }],
    'locations': [{
        'base_col': 'V2ENHANCEDLOCATIONS',
        'regexp': r"([^#;]+(?:#[^#;]+)*);?",
        'cols': [
            'location_type',
            'location_full_name',
            'location_country_code',
            'location_adm1_code',
            'location_latitude',
            'location_longitude',
            'location_feature_id',
        ]
    }],
    'persons': [{
        'base_col': 'V2ENHANCEDPERSONS',
        'regexp': r'([^,;]+(?:,[^,;]+)*);?',
        'cols': [
            'person_name',
            'char_offset'
        ]
    }],
    'organizations': [{
        'base_col': 'V2ENHANCEDORGANIZATIONS',
        'regexp': r'([^,;]+(?:,[^,;]+)*);?',
        'cols': [
            'organization_name',
            'char_offset'
        ]
    }],
    'articles': [{
        'base_col': 'V1.5TONE',
        'regexp': r'^(-?\d+(?:\.\d+)?),(-?\d+(?:\.\d+)?),(-?\d+(?:\.\d+)?),(-?\d+(?:\.\d+)?),(-?\d+(?:\.\d+)?),(-?\d+(?:\.\d+)?),(-?\d+(?:\.\d+)?)$',
        'cols': [
            {'name': 'tone', 'type': 'DOUBLE'},
            {'name': 'positive_score', 'type': 'DOUBLE'},
            {'name': 'negative_score', 'type': 'DOUBLE'},
            {'name': 'polarity', 'type': 'DOUBLE'},
            {'name': 'activity_reference_density', 'type': 'DOUBLE'},
            {'name': 'self_group_reference_density', 'type': 'DOUBLE'},
            {'name': 'word_count', 'type': 'INT64'},
        ]
    }],
    'dates': [{
        'base_col': 'V2.1ENHANCEDDATES',
        'regexp': r'([^#;]+(?:#[^#;]+)*);?',
        'cols': [
            'date_resolution',
            'month',
            'day',
            'year',
            'char_offset'
        ]
    }],
    'related_images': [{
        'base_col': 'V2.1RELATEDIMAGES',
        'regexp': r'[^;]+(?:;[^;]+)*',
        'cols': [
            'url'
        ]
    }],
    'social_image_embeds': [{
        'base_col': 'V2.1SOCIALIMAGEEMBEDS',
        'regexp': r'[^;]+(?:;[^;]+)*',
        'cols': [
            'url'
        ]
    }],
    'social_video_embeds': [{
        'base_col': 'V2.1SOCIALVIDEOEMBEDS',
        'regexp': r'[^;]+(?:;[^;]+)*',
        'cols': [
            'url'
        ]
    }],
    'quotations': [{
        'base_col': 'V2.1QUOTATIONS',
        'regexp': r'[^#]+(?:\|[^#]+)*(?:#[^#]+(?:\|[^#]+)*)*',
        'cols': [
            'char_offset',
            'length',
            'verb',
            'quote',
        ]
    }],
    'all_names': [{
        'base_col': 'V2.1ALLNAMES',
        'regexp': r'([^,;]+(?:,[^,;]+)*);?',
        'cols': [
            'name',
            'char_offset'
        ]
    }],
    'amounts': [{
        'base_col': 'V2.1AMOUNTS',
        'regexp': r'([^,;]+(?:,[^,;]+)*);?',
        'cols': [
            'amount',
            'object',
            'char_offset'
        ]
    }]
}

FILTER_COLUMNS = [
    'V1COUNTS',
    'V1THEMES',
]

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

def csv_clean(tsv_stream):
    content = tsv_stream.read().decode('latin-1')
    processed_content = content.replace('"', '')
    return io.BytesIO(processed_content.encode('latin-1'))

def tsv_to_table(tsv_file) -> pa.Table:
    tsv_file = csv_clean(tsv_file)
    parse_options = pv.ParseOptions(delimiter='\t')
    read_options = pv.ReadOptions(encoding='latin1', column_names=GKG_HEADERS)
    convert_options = pv.ConvertOptions(
        column_types=GKG_DICT,
        include_columns=USED_GKG_HEADERS,
        include_missing_columns=True,
        timestamp_parsers=['%Y%m%d%H%M%S'],
        strings_can_be_null=True,
        null_values=['']
    )
    gkg_table = pv.read_csv(tsv_file, read_options=read_options, parse_options=parse_options, convert_options=convert_options)

    # removing trailing commas from final column (V2EXTRASXML)
    clean_xml = pa.compute.utf8_rtrim(gkg_table['V2EXTRASXML'], characters=',')
    gkg_table = gkg_table.set_column(
        gkg_table.column_names.index('V2EXTRASXML'), 
        'V2EXTRASXML', 
        clean_xml
    )

    return gkg_table

def create_base_table(link: str) -> tuple[pa.Table, pa.Table]:
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
        with z.open(csv_file_name, 'rb') as f:
            if '.gkg.' in link:
                gkg_table = tsv_to_table(f)
    return gkg_table

def create_table(
    gkg_table: pa.Table,
    con: duckdb.DuckDBPyConnection, 
    base_cols: dict,
    regex_cols: list[dict] = None
) -> pa.Table:
    """Extract table from GKG table."""
    query_str = """SELECT """
    cast_query_str = """SELECT """
    for item in base_cols:
        base_col = item[0]
        col_name = item[1]
        query_str += f"{base_col} AS {col_name}, "
        cast_query_str += f"{col_name}, "
    if regex_cols is not None:
        for item in regex_cols:
            base_col = item["base_col"]
            regexp = item["regexp"]
            cols = item["cols"]
            col_names = [f"'{col['name']}'" for col in cols]
            cast_cols = [f"CAST({col['name']} AS {col['type']}) AS {col['name']}" for col in cols]
            cast_query_str += ", ".join(cast_cols) + " "
            params_str = "'" + regexp + "'" + ', ' + '[' + ', '.join(col_names) + ']'
            query_str += f"""unnest(regexp_extract("{base_col}", {params_str})) """
    else:
        query_str = query_str.rstrip(", ")
        cast_query_str = cast_query_str.rstrip(", ")
    query_str += "FROM gkg_table;"
    cast_query_str += "FROM pre_cast_table;"
    pre_cast_table = con.execute(query_str).arrow()
    table = con.execute(cast_query_str).arrow()  

    return table

def extract_tables(src_table: pa.Table) -> list[pa.Table]:
    con = duckdb.connect()
    table_dict = {}
    for name in TABLE_LIST:
        base_cols = BASE_COLS[name]
        regex_cols = STR_REGEX_DICT[name]
        table = create_table(src_table, con, regex_cols, base_cols)
        table_dict[name] = table
    return table_dict


def to_s3(table: pa.Table, bucket: str, name: str, con) -> bool:
    buffer = io.BytesIO()
    pq.write_table(table, buffer)

    buffer.seek(0)

    client = boto3.client('s3')
    file_name = name + '.parquet'

    try:
        _ = client.put_object(Bucket=bucket, Key=file_name, Body=buffer.getvalue())
    except botocore.exceptions.ClientError as error:
        raise error
    except botocore.exceptions.ParamValidationError as error:
        raise ValueError('The parameters you provided are incorrect: {}'.format(error))
    return True

def upload(topic_df: pd.DataFrame, bucket: str, name: str) -> bool:
    """Upload table to S3."""
    df_put = to_s3(topic_df, bucket, name)
    return df_put

def add_file(table: pa.Table):
    catalog = catalog.load_catalog('default')
    