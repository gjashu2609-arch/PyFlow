# pyflow/transformers.py

import pandas as pd
import numpy as np
import logging
from PyFlow.exceptions_custom import TransformationError
from PyFlow.utils import timing_decorator

logger = logging.getLogger(__name__)


@timing_decorator
def load_optimized_csv(filepath: str) -> pd.DataFrame:

    dtypes = {
        "VendorID":            "int8",
        "passenger_count":     "float32",
        "trip_distance":       "float32",
        "RatecodeID":          "float32",
        "PULocationID":        "int16",
        "DOLocationID":        "int16",
        "payment_type":        "int8",
        "fare_amount":         "float32",
        "extra":               "float32",
        "mta_tax":             "float32",
        "tip_amount":          "float32",
        "tolls_amount":        "float32",
        "improvement_surcharge":"float32",
        "total_amount":        "float32",
        "congestion_surcharge": "float32",
    }

    category_cols = ["VendorID", "payment_type", "RatecodeID"]

    try:
        df = pd.read_csv(
            filepath,
            dtype=dtypes,
            parse_dates=["tpep_pickup_datetime", "tpep_dropoff_datetime"],
            low_memory=False
        )
        for col in category_cols:
            if col in df.columns:
                df[col] = df[col].astype("category")

        logger.info(f"Loaded: {df.shape}, Memory: {df.memory_usage(deep=True).sum() / 1e6:.1f} MB")
        return df

    except FileNotFoundError:
        raise TransformationError(f"File not found: {filepath}")
    except Exception as e:
        raise TransformationError(f"Error loading CSV: {e}")
@timing_decorator
def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for col in df.columns:
        missing = df[col].isna().sum()
        if missing == 0:
            continue

        logger.info(f"Column '{col}': {missing} missing values")

        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].ffill()
            logger.info(f"  '{col}' → forward-filled")
        elif pd.api.types.is_numeric_dtype(df[col]):
            skewness = df[col].skew()
            if abs(skewness) > 1:
                fill_val = df[col].median()
                method = "median"
            else:
                fill_val = df[col].mean()
                method = "mean"
            df[col] = df[col].fillna(round(fill_val, 2))
            logger.info(f"  '{col}' → filled with {method}: {fill_val:.2f}")

        else:
            mode_vals = df[col].mode()
            fill_val = mode_vals[0] if len(mode_vals) > 0 else "Unknown"
            df[col] = df[col].fillna(fill_val)
            logger.info(f"  '{col}' → filled with mode: '{fill_val}'")

    return df
@timing_decorator
def handle_outliers_iqr(
    df: pd.DataFrame,
    column: str,
    multiplier: float = 1.5
) -> pd.DataFrame:
   
    df = df.copy()

    Q1  = df[column].quantile(0.25)
    Q3  = df[column].quantile(0.75)
    IQR = Q3 - Q1

    lower = Q1 - (multiplier * IQR)
    upper = Q3 + (multiplier * IQR)

    outliers = df[(df[column] < lower) | (df[column] > upper)]
    logger.info(f"'{column}': {len(outliers)} outliers removed (bounds: {lower:.2f} to {upper:.2f})")

    return df[(df[column] >= lower) & (df[column] <= upper)]
@timing_decorator
def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    
    business_keys = [
        "tpep_pickup_datetime",
        "PULocationID",
        "DOLocationID"
    ]

    keys = [k for k in business_keys if k in df.columns]

    before = len(df)
    df = df.drop_duplicates(subset=keys, keep="first")
    after  = len(df)

    logger.info(f"Removed {before - after} duplicates ({before} → {after} rows)")
    return df
@timing_decorator
def merge_trip_data(
    trips_df: pd.DataFrame,
    weather_df: pd.DataFrame,
    holidays_df: pd.DataFrame
) -> pd.DataFrame:
   
    before = len(trips_df)

    trips_df = trips_df.copy()
    trips_df["date"] = pd.to_datetime(
        trips_df["tpep_pickup_datetime"]
    ).dt.date.astype(str)

    merged = trips_df.merge(weather_df, on="date", how="left")
    logger.info(f"After weather merge: {len(merged)} rows (was {before})")

    merged = merged.merge(holidays_df, on="date", how="left")
    logger.info(f"After holiday merge: {len(merged)} rows")

    if len(merged) != before:
        raise TransformationError(
            f"Merge created extra rows: {before} → {len(merged)}. "
            f"Check for duplicate keys in weather or holidays data."
        )

    logger.info("Merge validation passed")
    return merged
@timing_decorator
def group_and_aggregate(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "tpep_pickup_datetime" in df.columns:
        df["date"]  = pd.to_datetime(df["tpep_pickup_datetime"]).dt.date
        df["hour"]  = pd.to_datetime(df["tpep_pickup_datetime"]).dt.hour

    result = df.groupby(["date", "hour"]).agg(
        trip_count    = ("fare_amount", "count"),
        avg_fare      = ("fare_amount", "mean"),
        total_revenue = ("fare_amount", "sum"),
        avg_distance  = ("trip_distance", "mean"),
        max_fare      = ("fare_amount", "max"),
        min_fare      = ("fare_amount", "min"),
    ).reset_index()

    result["avg_fare"] = result["avg_fare"].round(2)
    logger.info(f"Grouped into {len(result)} time buckets")
    return result
@timing_decorator
def extract_datetime_features(df: pd.DataFrame) -> pd.DataFrame:
    holidays = [
        "2024-01-01",  
        "2024-07-04", 
        "2024-12-25",  
    ]

    df = df.copy()
    df["tpep_pickup_datetime"]  = pd.to_datetime(df["tpep_pickup_datetime"],  errors="coerce")
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"], errors="coerce")

    df["hour"]              = df["tpep_pickup_datetime"].dt.hour
    df["day_of_week"]       = df["tpep_pickup_datetime"].dt.day_name()
    df["is_weekend"]        = df["tpep_pickup_datetime"].dt.dayofweek >= 5
    df["date_str"]          = df["tpep_pickup_datetime"].dt.strftime("%Y-%m-%d")
    df["is_holiday"]        = df["date_str"].isin(holidays)
    df["trip_duration_mins"] = (
        df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
    ).dt.total_seconds() / 60

    logger.info("Datetime features extracted: hour, day_of_week, is_weekend, is_holiday, trip_duration_mins")
    return df
@timing_decorator
def rolling_trip_average(
    df: pd.DataFrame,
    window_days: int = 7
) -> pd.DataFrame:
    df = df.copy()
    df["date"] = pd.to_datetime(df["tpep_pickup_datetime"]).dt.date

    daily = df.groupby(["date", "PULocationID"]).size().reset_index(name="trip_count")
    daily["date"] = pd.to_datetime(daily["date"])
    daily = daily.sort_values(["PULocationID", "date"])
    daily["rolling_avg"] = (
        daily.groupby("PULocationID")["trip_count"]
        .transform(lambda x: x.rolling(window=window_days, min_periods=1).mean())
        .round(1)
    )

    logger.info(f"{window_days}-day rolling average calculated for {daily['PULocationID'].nunique()} locations")
    return daily
@timing_decorator
def filter_rush_hour_trips(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "hour" not in df.columns:
        df = extract_datetime_features(df)

    df["is_weekday"] = ~df["is_weekend"]
    result = df.query(
        "(hour >= 7 and hour <= 10 or hour >= 16 and hour <= 19) "
        "and is_weekday == True "
        "and fare_amount > 20"
    )

    logger.info(f"Rush hour trips with fare > $20: {len(result)} rows")
    return result
@timing_decorator
def export_dataframe(
    df: pd.DataFrame,
    output_dir: str = "data/output"
) -> dict[str, str]:

    import os
    os.makedirs(output_dir, exist_ok=True)

    paths = {}

    # CSV with gzip compression
    csv_path = f"{output_dir}/trips.csv.gz"
    df.to_csv(csv_path, index=False, compression="gzip")
    paths["csv"] = csv_path
    logger.info(f"Saved CSV: {csv_path}")

    # JSON 
    json_path = f"{output_dir}/trips.json"
    df.to_json(json_path, orient="records", indent=2)
    paths["json"] = json_path
    logger.info(f"Saved JSON: {json_path}")

    # Parquet
    parquet_path = f"{output_dir}/trips.parquet"
    df.to_parquet(parquet_path, index=False, compression="snappy")
    paths["parquet"] = parquet_path
    logger.info(f"Saved Parquet: {parquet_path}")

    # Excel
    excel_path = f"{output_dir}/trips.xlsx"
    #Excel has 1M row limit, export first 1M rows only
    excel_df = df.head(1048575)
    excel_df.to_excel(excel_path, index=False, engine="openpyxl")
    logger.warning(f"Excel limited to 1,048,576 rows (dataset has {len(df)} rows)")

    return paths

class DataTransformer:
 
    def __init__(self, config: dict) -> None:
        self.config = config

    @timing_decorator
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
    
        logger.info(f"Starting transformation: {df.shape[0]} rows")

       
        df = handle_missing_values(df)
        logger.info("Missing values handled")

        if "fare_amount" in df.columns:
            df = handle_outliers_iqr(df, "fare_amount")
            logger.info("Outliers removed")

        df = remove_duplicates(df)
        logger.info("Duplicates removed")
        if "tpep_pickup_datetime" in df.columns:
            df = extract_datetime_features(df)
            logger.info("Datetime features extracted")

        logger.info(f"Transformation complete: {df.shape[0]} rows remaining")
        return df