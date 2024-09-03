import pytest
import pandas as pd
import numpy as np
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.etl import check_links, gkg_process, GKG_HEADERS, update_scores

def test_check_links():
    """Good link sets are accepted and bad link sets are rejected."""
    good_link_dict = {'gkg': 'http://data.gdeltproject.org/gdeltv2/20240511013000.gkg.csv.zip',
                      'export': 'http://data.gdeltproject.org/gdeltv2/20240511013000.export.CSV.zip',
                      'mentions': 'http://data.gdeltproject.org/gdeltv2/20240511013000.mentions.CSV.zip'}
    bad_link_dict = {}
    assert check_links(good_link_dict)
    assert not check_links(bad_link_dict)

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
            'Robert Kavcic,530;Hassan Pirnia,"1607',
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
            '''Fifth-place Roma,17;Romelu Lukaku,149''',
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
            'Robert Kavcic,530;Hassan Pirnia,"1607;Test Name,1234',
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
            '''Fifth-place Roma,17;Romelu Lukaku,149''',
            'col25',
            'col26',
            'col27',
        ]
    ]
    test_df = pd.DataFrame(data, columns=GKG_HEADERS)
    # will probably neet to change as data needs are changed
    url_df_cols = ['topic', 'sources', 'urls']
    
    expected_rows = [[
        'Robert Kavcic',
        ['apnews.com', 'reuters.com'],
        ['http://apnews.com/fake/url/test', 'http://reuters.com/fake/url/test'],
    ], [
        'Hassan Pirnia',
        ['apnews.com', 'reuters.com'],
        ['http://apnews.com/fake/url/test', 'http://reuters.com/fake/url/test'],
    ], [
        'Youtube',
        ['apnews.com'],
        ['http://apnews.com/fake/url/test'],
    ], [
        'Test Name',
        ['reuters.com'],
        ['http://reuters.com/fake/url/test'],
    ], [
        'Fifth-place Roma',
        ['apnews.com', 'reuters.com'],
        ['http://apnews.com/fake/url/test', 'http://reuters.com/fake/url/test'],
    ], [
        'Romelu Lukaku',
        ['apnews.com', 'reuters.com'],
        ['http://apnews.com/fake/url/test', 'http://reuters.com/fake/url/test'],
    ]]

    expected_src_rows = [
        ['Robert Kavcic', 'apnews.com', 1],
        ['Robert Kavcic', 'reuters.com', 1],
        ['Hassan Pirnia', 'apnews.com', 1],
        ['Hassan Pirnia', 'reuters.com', 1],
        ['Test Name', 'reuters.com', 1],
        ['Youtube', 'apnews.com', 1],
        ['Fifth-place Roma', 'apnews.com', 1],
        ['Fifth-place Roma', 'reuters.com', 1],
        ['Romelu Lukaku', 'apnews.com', 1],
        ['Romelu Lukaku', 'reuters.com', 1]
    ]
    expected_url_df = pd.DataFrame(expected_rows, columns=url_df_cols)
    expected_src_df = pd.DataFrame(expected_src_rows, columns=['topic', 'source', 'count'])
    actual_df, actual_src_df = gkg_process(test_df)
    for _, actual_row in actual_df.iterrows():
        assert expected_url_df[expected_url_df['topic'] == actual_row['topic']][url_df_cols].squeeze().equals(actual_row)
    for _, actual_row in actual_src_df.iterrows():
        assert expected_src_df[(expected_src_df['topic'] == actual_row['topic']) & (expected_src_df['source'] == actual_row['source'])].squeeze().equals(actual_row)

@pytest.fixture
def new_topic_srcs():
    new_topic_srcs_rows = [
        ['Robert Kavcic', 'apnews.com', 1],
        ['Robert Kavcic', 'reuters.com', 1],
        ['Hassan Pirnia', 'apnews.com', 1],
        ['Hassan Pirnia', 'reuters.com', 1],
        ['Test Name', 'reuters.com', 1],
        ['Youtube', 'apnews.com', 1],
        ['Fifth-place Roma', 'apnews.com', 1],
        ['Fifth-place Roma', 'reuters.com', 1],
        ['Romelu Lukaku', 'apnews.com', 1],
        ['Romelu Lukaku', 'reuters.com', 1]
    ]
    return pd.DataFrame(new_topic_srcs_rows, columns=['topic', 'source', 'count'])

@pytest.fixture
def new_urls():
    new_data = [[
        'Robert Kavcic',
        ['apnews.com', 'reuters.com'],
        ['http://apnews.com/fake/url/test', 'http://reuters.com/fake/url/test'],
    ], [
        'Hassan Pirnia',
        ['apnews.com', 'reuters.com'],
        ['http://apnews.com/fake/url/test', 'http://reuters.com/fake/url/test'],
    ], [
        'Youtube',
        ['apnews.com'],
        ['http://apnews.com/fake/url/test'],
    ], [
        'Test Name',
        ['reuters.com'],
        ['http://reuters.com/fake/url/test'],
    ], [
        'Fifth-place Roma',
        ['apnews.com', 'reuters.com'],
        ['http://apnews.com/fake/url/test', 'http://reuters.com/fake/url/test'],
    ], [
        'Romelu Lukaku',
        ['apnews.com', 'reuters.com'],
        ['http://apnews.com/fake/url/test', 'http://reuters.com/fake/url/test'],
    ]]
    return pd.DataFrame(new_data, columns=['topic', 'source', 'urls'])

def score_func(count: int, src_count: int) -> float:
    return np.log(count) + 2 * np.log(src_count) + 1

@pytest.fixture
def scores_df():
    scores_rows = [
        ['Robert Kavcic', 10, 5, score_func(10, 5)],
        ['Hassan Pirnia', 3, 3, score_func(3, 3)],
        ['Fifth-place Roma', 10, 6, score_func(10, 6)],
        ['Arizona', 8, 4, score_func(8, 4)],
        ['Vermont', 1, 1, score_func(1, 1)],
        ['John Doe', 3, 2, score_func(1, 1)],
    ]
    cols = ['topic', 'count', 'src_count', 'score']
    return pd.DataFrame(scores_rows, columns=cols)

@pytest.fixture
def topic_src_df():
    srcs_rows = [
        ['Robert Kavcic', 'apnews.com', 2],
        ['Robert Kavcic', 'news1.com', 2],
        ['Robert Kavcic', 'news2.com', 2],
        ['Robert Kavcic', 'news3.com', 2],
        ['Robert Kavcic', 'news4.com', 2],
        ['Hassan Pirnia', 'reuters.com', 1],
        ['Hassan Pirnia', 'news1.com', 1],
        ['Hassan Pirnia', 'news2.com', 1],
        ['Fifth-place Roma', 'apnews.com', 3],
        ['Fifth-place Roma', 'news1.com', 3],
        ['Fifth-place Roma', 'news2.com', 1],
        ['Fifth-place Roma', 'news3.com', 1],
        ['Fifth-place Roma', 'news4.com', 1],
        ['Fifth-place Roma', 'news5.com', 1],
        ['Test Name', 'reuters.com', 2],
        ['Test Name', 'news1.com', 2],
        ['Test Name', 'news2.com', 2],
        ['Test Name', 'news3.com', 2],
        ['Arizona', 'apnews.com', 3],
        ['Arizona', 'news1.com', 2],
        ['Arizona', 'news2.com', 2],
        ['Arizona', 'news3.com', 1],
        ['Vermont', 'reuters.com', 1],
        ['John Doe', 'news1.com', 1],
        ['John Doe', 'news2.com', 2],
    ]
    cols = ['topic', 'source', 'topic_src_counts']
    return pd.DataFrame(srcs_rows, columns=cols)

@pytest.fixture
def old_df():
    old_rows = [
        ['Vermont', ['reuters.com'], ['reuters.com/fake1'], 1],
        ['Arizona', ['apnews.com/fake1', 'apnews.com/fake2', 'news1.com/az'], 3],
    ]
    return pd.DataFrame(old_rows, columns=['topic', 'source', 'urls', 'count'])

@pytest.fixture
def old_src_df():
    old_src_rows = [
        ['Vermont', 'reuters.com', 1],
        ['Arizona', 'apnews.com', 2],
        ['Arizona', 'news1.com', 1],
    ]
    cols = ['topic', 'source', 'count']
    return pd.DataFrame(old_src_rows, columns=cols)

@pytest.fixture
def expected_scores_df():
    expected_scores_rows = [
        ['Robert Kavcic', 6, 12, score_func(12, 6)],
        ['Hassan Pirnia', 4, 5, score_func(5, 4)],
        ['Fifth-place Roma', 7, 12, score_func(12, 7)],
        ['Test Name', 4, 9, score_func(9, 4)],
        ['Arizona', 4, 5, score_func(5, 4)],
        ['John Doe', 2, 3, score_func(3, 2)],
        ['Youtube', 1, 1, score_func(1, 1)],
        ['Romelu Lukaku', 2, 2, score_func(2, 2)]
    ]
    cols = ['topic', 'src_count', 'count', 'score']
    return pd.DataFrame(expected_scores_rows, columns=cols)

@pytest.fixture
def expected_src_df():
    srcs_rows = [
        ['Robert Kavcic', 'apnews.com', 3],
        ['Robert Kavcic', 'reuters.com', 1],
        ['Robert Kavcic', 'news1.com', 2],
        ['Robert Kavcic', 'news2.com', 2],
        ['Robert Kavcic', 'news3.com', 2],
        ['Robert Kavcic', 'news4.com', 2],
        ['Hassan Pirnia', 'reuters.com', 2],
        ['Hassan Pirnia', 'apnews.com', 1],
        ['Hassan Pirnia', 'news1.com', 1],
        ['Hassan Pirnia', 'news2.com', 1],
        ['Fifth-place Roma', 'apnews.com', 4],
        ['Fifth-place Roma', 'news1.com', 3],
        ['Fifth-place Roma', 'news2.com', 1],
        ['Fifth-place Roma', 'news3.com', 1],
        ['Fifth-place Roma', 'news4.com', 1],
        ['Fifth-place Roma', 'news5.com', 1],
        ['Fifth-place Roma', 'reuters.com', 1],
        ['Test Name', 'reuters.com', 3],
        ['Test Name', 'news1.com', 2],
        ['Test Name', 'news2.com', 2],
        ['Test Name', 'news3.com', 2],
        ['Arizona', 'apnews.com', 1],
        ['Arizona', 'news1.com', 1],
        ['Arizona', 'news2.com', 2],
        ['Arizona', 'news3.com', 1],
        ['John Doe', 'news1.com', 1],
        ['John Doe', 'news2.com', 2],
        ['Youtube', 'apnews.com', 1],
        ['Romelu Lukaku', 'apnews.com', 1],
        ['Romelu Lukaku', 'reuters.com', 1]
    ]
    cols = ['topic', 'source', 'count']
    return pd.DataFrame(srcs_rows, columns=cols)


def test_update(
    new_topic_srcs: pd.DataFrame,
    topic_src_df: pd.DataFrame,
    scores_df: pd.DataFrame,
    old_src_df: pd.DataFrame,
    expected_src_df: pd.DataFrame,
    expected_scores_df: pd.DataFrame
):
    """update_scores has the intenden result."""
    actual_scores_df, actual_src_df = update_scores(new_topic_srcs, topic_src_df, old_src_df)
    actual_src_df = actual_src_df.sort_values(by=list(actual_src_df.columns)).reset_index(drop=True)
    expected_src_df = expected_src_df.astype(actual_src_df.dtypes.to_dict())
    expected_src_df = expected_src_df.sort_values(by=list(expected_src_df.columns)).reset_index(drop=True)

    assert actual_src_df.equals(expected_src_df)

    actual_scores_df = actual_scores_df.sort_values(by=list(actual_scores_df.columns)).reset_index(drop=True)
    expected_scores_df = expected_scores_df.sort_values(by=list(expected_scores_df.columns)).reset_index(drop=True)
    expected_scores_df = expected_scores_df.astype(actual_scores_df.dtypes.to_dict())

    assert actual_scores_df.equals(expected_scores_df)
