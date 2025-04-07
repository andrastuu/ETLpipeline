import pytest
import pandas as pd
from etl.transform import transform

@pytest.fixture
def realistic_data():
    conversions = pd.DataFrame({
        "country_name": ["Poland"],
        "created_at": ["2024-04-22 12:00:00"]
    })

    # 2024-04-22 12:00:00 UTC → timestamp = 1713787200
    broker_data = pd.DataFrame({
        "timestamp": [1713787200],
        "ip_country": ["Poland"],
        "important_score": [1]
    })

    return conversions, broker_data

def test_transform_structure(realistic_data):
    conversions, broker_data = realistic_data
    df = transform(conversions, broker_data)

    assert isinstance(df, pd.DataFrame)
    assert "created_at" in df.columns
    assert "broker_timestamp" in df.columns
    assert "country_name" in df.columns

def test_transform_no_crash_on_valid_input(realistic_data):
    conversions, broker_data = realistic_data
    df = transform(conversions, broker_data)
    assert not df.empty

def test_transform_handles_unmatchable_country():
    conversions = pd.DataFrame({
        "country_name": ["Wakanda"],
        "created_at": ["2024-04-01 00:00:00"]
    })
    broker_data = pd.DataFrame({
        "timestamp": [1713716000],
        "ip_country": ["Atlantis"],
        "important_score": [1]
    })

    df = transform(conversions, broker_data)
    assert df["broker_timestamp"].isna().all(), "Unmatched country should yield null timestamp"
