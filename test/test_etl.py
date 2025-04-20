import pytest
import os
import duckdb
import pyarrow.csv as pv
import pyarrow as pa
from src.etl import tsv_to_table, create_table, extract_tables, TABLE_LIST, BASE_COLS, STR_REGEX_DICT
import traceback
import io

def test_create_table(article_type_dict):
    file_path = os.path.dirname(os.path.abspath(__file__))
    con = duckdb.connect()
    with open(f'{file_path}/data/test_input.csv', 'rb') as f:
        gkg_table = tsv_to_table(f)
    for name in TABLE_LIST:
        actual_table = create_table(gkg_table, con, BASE_COLS[name], STR_REGEX_DICT[name])
        with open(f'{file_path}/data/expected/{name}.csv', 'rb') as f:
            parse_options = pv.ParseOptions(delimiter=',')
            read_options = pv.ReadOptions(encoding='latin1')
            convert_options = pv.ConvertOptions(
                include_missing_columns=True,
                timestamp_parsers=['%Y%m%d%H%M%S'],
                strings_can_be_null=True,
                column_types = article_type_dict,
                null_values=['']
            )
            expected_table = pv.read_csv(f, read_options=read_options, convert_options=convert_options, parse_options = parse_options)
        pytest.set_trace()
        assert actual_table.equals(expected_table)
    