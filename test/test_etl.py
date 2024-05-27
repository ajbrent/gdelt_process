import pytest
import pandas as pd
import numpy as np
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.etl import check_links, gkg_process, GKG_HEADERS

def test_check_links():
    """Testing that good link sets are accepted and bad link sets are rejected."""
    good_link_dict = {'gkg': 'http://data.gdeltproject.org/gdeltv2/20240511013000.gkg.csv.zip',
                      'export': 'http://data.gdeltproject.org/gdeltv2/20240511013000.export.CSV.zip',
                      'mentions': 'http://data.gdeltproject.org/gdeltv2/20240511013000.mentions.CSV.zip'}
    bad_link_dict = {}
    assert check_links(good_link_dict)
    assert not check_links(bad_link_dict)

def score_calc(count: int, src_count: int) -> float:
    """Calculate the score for a given topic."""
    return np.log(count) + 2 * np.log(src_count) + 1
def test_gkg_process():
    data = [
        [
            'col1',
            20240520153000,
            1,
            'apnews.com',
            'http://apnews.com/fake/url/test',
            'col6',
            'col7',
            'col8',
            'col9',
            'col10',
            '1#China#CH#CH#35#105#CH',
            'col12',
            'Robert Kavcic",530;Hassan Pirnia,"1607',
            'col14',
            'Youtube,431',
            'col16',
            'col17',
            'col18',
            'col19',
            'col20',
            'col21',
            'col22',
            'col23',
            '''Fifth-place Roma",17;Romelu Lukaku,149''',
            'col25',
            'col26',
            'col27',
        ],
        [
            'col1',
            20240520153000,
            1,
            'reuters.com',
            'http://reuters.com/fake/url/test',
            'col6',
            'col7',
            'col8',
            'col9',
            'col10',
            '1#China#CH#CH#35#105#CH',
            'col12',
            'Robert Kavcic",530;Hassan Pirnia,"1607;Test Name,1234',
            'col14',
            '',
            'col16',
            'col17',
            'col18',
            'col19',
            'col20',
            'col21',
            'col22',
            'col23',
            '''Fifth-place Roma",17;Romelu Lukaku,149''',
            'col25',
            'col26',
            'col27',
        ]
    ]
    test_df = pd.DataFrame(data, columns=GKG_HEADERS)
    # will probably neet to change as data needs are changed
    df_cols = ['topics', 'sources', 'urls', 'counts', 'src_counts', 'scores']
    
    expected_rows = [[
        'Robert Kavcic"',
        ['apnews.com', 'reuters.com'],
        ['http://apnews.com/fake/url/test', 'http://reuters.com/fake/url/test'],
        2,
        2,
        score_calc(2, 2)
    ], [
        'Hassan Pirnia',
        ['apnews.com', 'reuters.com'],
        ['http://apnews.com/fake/url/test', 'http://reuters.com/fake/url/test'],
        2,
        2,
        score_calc(2, 2)
    ], [
        'Youtube',
        ['apnews.com'],
        ['http://apnews.com/fake/url/test'],
        1,
        1,
        score_calc(1, 1)
    ], [
        'Test Name',
        ['reuters.com'],
        ['http://reuters.com/fake/url/test'],
        1,
        1,
        score_calc(1, 1)
    ], [
        'Fifth-place Roma"',
        ['apnews.com', 'reuters.com'],
        ['http://apnews.com/fake/url/test', 'http://reuters.com/fake/url/test'],
        2,
        2,
        score_calc(2, 2)
    ], [
        'Romelu Lukaku',
        ['apnews.com', 'reuters.com'],
        ['http://apnews.com/fake/url/test', 'http://reuters.com/fake/url/test'],
        2,
        2,
        score_calc(2, 2)
    ]]
    expected_df = pd.DataFrame(expected_rows, columns=df_cols)
    actual_df = gkg_process(test_df)
    for _, actual_row in actual_df.iterrows():
        pytest.set_trace()
        assert expected_df[expected_df['topics'] == actual_row['topics']][df_cols].squeeze().equals(actual_row)