
import pytest
import pyarrow as pa

@pytest.fixture
def article_type_dict():
    return {
        "record_id": pa.string(),
        "collection_timestamp": pa.timestamp('s'),
        "src_collection_identifier": pa.uint16(),
        "src_common_name": pa.string(),
        "doc_identifier": pa.string(),
        "tone": pa.float64(),
        "positive_score": pa.float64(),
        "negative_score": pa.float64(),
        "polarity": pa.float64(),
        "activity_reference_density": pa.float64(),
        "self_group_reference_density": pa.float64(),
        "word_count": pa.int64()
    }