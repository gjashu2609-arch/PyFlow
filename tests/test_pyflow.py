
import pytest
import pandas as pd
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from PyFlow.exceptions_custom import (
    PyFlowError, DataSourceError,
    ValidationError, TransformationError, LoadError
)
from PyFlow.utils import load_config, extract_email_domains, get_top_domains
from PyFlow.transformers import (
    handle_missing_values,
    handle_outliers_iqr,
    remove_duplicates,
    extract_datetime_features,
    group_and_aggregate
)

@pytest.fixture
def sample_df():
   
    return pd.DataFrame({
        "tpep_pickup_datetime":  [
            "2024-01-01 08:00:00",
            "2024-01-01 08:00:00",
            "2024-01-06 17:00:00",
        ],
        "tpep_dropoff_datetime": [
            "2024-01-01 08:25:00",
            "2024-01-01 08:25:00",
            "2024-01-06 17:45:00",
        ],
        "PULocationID":  [100, 100, 200],
        "DOLocationID":  [200, 200, 300],
        "fare_amount":   [12.5, 12.5, 8.0],
        "trip_distance": [3.2,  3.2,  2.1],
    })


@pytest.fixture
def df_with_missing():
    return pd.DataFrame({
        "fare_amount":   [10.0, None, 15.0, None, 8.0],
        "pickup":        ["Manhattan", None, "Brooklyn", None, "Queens"],
        "trip_distance": [1.2, 3.4, None, 2.1, None],
    })


@pytest.fixture
def df_with_outliers():
   
    return pd.DataFrame({
        "fare_amount": [5, 8, 10, 12, 9, 7, 500, -50, 11, 6]
    })


@pytest.fixture
def customer_data():
    return [
        {"email": "alice@gmail.com"},
        {"email": "bob@yahoo.com"},
        {"email": "carol@gmail.com"},
        {"email": "dan@company.org"},
        {"email": "not-an-email"},
        {"name": "no email here"},
    ]


class TestExceptions:

    def test_pyflow_error_is_base(self):
        assert issubclass(DataSourceError,     PyFlowError)
        assert issubclass(ValidationError,     PyFlowError)
        assert issubclass(TransformationError, PyFlowError)
        assert issubclass(LoadError,           PyFlowError)

    def test_exceptions_can_be_raised(self):
        with pytest.raises(DataSourceError):
            raise DataSourceError("file not found")

    def test_catch_with_base_class(self):
        with pytest.raises(PyFlowError):
            raise LoadError("db failed")

    def test_exception_message(self):
        err = ValidationError("bad email format")
        assert "bad email format" in str(err)



class TestConfig:

    def test_load_yaml_config(self, tmp_path):
        
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(
            "etl_settings:\n  chunk_size: 50000\n"
        )
        config = load_config(str(config_file))
        assert config["etl_settings"]["chunk_size"] == 50000

    def test_load_json_config(self, tmp_path):

        import json
        config_file = tmp_path / "test_config.json"
        config_file.write_text(json.dumps({"chunk_size": 1000}))
        config = load_config(str(config_file))
        assert config["chunk_size"] == 1000

    def test_missing_config_raises_error(self):
        with pytest.raises(DataSourceError):
            load_config("nonexistent_config.yaml")

    def test_unsupported_format_raises_error(self, tmp_path):
        bad_file = tmp_path / "config.txt"
        bad_file.write_text("key: value")
        with pytest.raises(DataSourceError):
            load_config(str(bad_file))


class TestMissingValues:

    def test_no_nulls_after_handling(self, df_with_missing):
        result = handle_missing_values(df_with_missing)
        assert result.isnull().sum().sum() == 0

    def test_numeric_filled_with_number(self, df_with_missing):
        result = handle_missing_values(df_with_missing)
        assert pd.api.types.is_numeric_dtype(result["fare_amount"])

    def test_categorical_filled_with_string(self, df_with_missing):
        result = handle_missing_values(df_with_missing)
        assert result["pickup"].notna().all()

    def test_row_count_preserved(self, df_with_missing):
        result = handle_missing_values(df_with_missing)
        assert len(result) == len(df_with_missing)

class TestOutliers:

    def test_outliers_removed(self, df_with_outliers):
        result = handle_outliers_iqr(df_with_outliers, "fare_amount")
        assert len(result) < len(df_with_outliers)

    def test_extreme_values_gone(self, df_with_outliers):
        result = handle_outliers_iqr(df_with_outliers, "fare_amount")
        assert result["fare_amount"].max() < 100
        assert result["fare_amount"].min() >= 0

    def test_original_df_unchanged(self, df_with_outliers):
        """handle_outliers_iqr should not modify original DataFrame."""
        original_len = len(df_with_outliers)
        handle_outliers_iqr(df_with_outliers, "fare_amount")
        assert len(df_with_outliers) == original_len



class TestDeduplication:

    def test_duplicates_removed(self, sample_df):
        result = remove_duplicates(sample_df)
        assert len(result) == 2 

    def test_unique_rows_kept(self, sample_df):
        result = remove_duplicates(sample_df)
        assert len(result) < len(sample_df)



class TestDatetimeFeatures:

    def test_hour_extracted(self, sample_df):
        result = extract_datetime_features(sample_df)
        assert "hour" in result.columns
        assert result["hour"].iloc[0] == 8

    def test_weekend_detected(self, sample_df):
        result = extract_datetime_features(sample_df)
        assert "is_weekend" in result.columns
        # 2024-01-06 is a Saturday
        assert result["is_weekend"].iloc[2] == True

    def test_holiday_detected(self, sample_df):
        result = extract_datetime_features(sample_df)
        assert "is_holiday" in result.columns
        # 2024-01-01 is New Year's Day
        assert result["is_holiday"].iloc[0] == True

    def test_duration_calculated(self, sample_df):
        result = extract_datetime_features(sample_df)
        assert "trip_duration_mins" in result.columns
        assert result["trip_duration_mins"].iloc[0] == 25.0

class TestEmailDomains:

    def test_domains_counted(self, customer_data):
        result = extract_email_domains(customer_data)
        assert result["gmail.com"] == 2

    def test_invalid_emails_skipped(self, customer_data):
        result = extract_email_domains(customer_data)
        total = sum(result.values())
        assert total == 4   # only 4 valid emails

    def test_top_domains_limited(self, customer_data):
        result = get_top_domains(customer_data, n=1)
        assert len(result) == 1
        assert "gmail.com" in result