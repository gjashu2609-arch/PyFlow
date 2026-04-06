import pandas as pd
from PyFlow.exceptions_custom import DataSourceError
import logging
from typing import Generator
import csv
from PyFlow.utils import timing_decorator
from collections import Counter,defaultdict,deque
import chardet
import json
import gzip
import zipfile
import io
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import threading
logger = logging.getLogger(__name__)


def read_csv_lines(filepath: str) -> Generator[dict, None, None]:
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)  
            for row in reader:
                yield dict(row)          

    except FileNotFoundError:
        raise DataSourceError(f"CSV file not found: {filepath}")
    except Exception as e:
        raise DataSourceError(f"Error reading CSV: {e}")


def read_csv_chunks(filepath: str, chunk_size: int = 100000) -> Generator[list, None, None]:

    try:
        chunk = []
        with open(filepath, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                chunk.append(dict(row))
                if len(chunk) == chunk_size:
                    yield chunk
                    chunk = []        
            if chunk:                   
                yield chunk

    except FileNotFoundError:
        raise DataSourceError(f"CSV file not found: {filepath}")
    
@timing_decorator
def top_pickup_locations(filepath: str, n: int = 10) -> list[tuple]:
    
    location_counts = Counter()

    for row in read_csv_lines(filepath):
        location = row.get("PULocationID", "unknown")
        location_counts[location] += 1

    return location_counts.most_common(n)
@timing_decorator
def avg_fare_by_hour(filepath: str) -> dict[str, float]:
   
    hour_fares: defaultdict = defaultdict(list)

    for row in read_csv_lines(filepath):
        pickup_time = row.get("tpep_pickup_datetime", "")
        fare = row.get("fare_amount", 0)
        try:
            hour = pickup_time.split(" ")[1].split(":")[0]
            hour_fares[hour].append(float(fare))
        except (IndexError, ValueError):
            continue   

    return {
        hour: round(sum(fares) / len(fares), 2)
        for hour, fares in sorted(hour_fares.items())
        if fares
    }
def create_rolling_buffer(max_size: int = 1000) -> deque:
    return deque(maxlen=max_size)


@timing_decorator
def fill_rolling_buffer(filepath: str, max_size: int = 1000) -> deque:
   
    buffer = create_rolling_buffer(max_size)

    for row in read_csv_lines(filepath):
        buffer.append(row)   # automatically drops oldest when full

    return buffer
@timing_decorator
def read_csv_chunked(filepath: str,chunk_size: int = 100000) -> Generator:
    
    try:
        chunk_iter = pd.read_csv(
            filepath,chunksize=chunk_size,low_memory=False
        )
        for chunk in chunk_iter:
            yield chunk

    except FileNotFoundError:
        raise DataSourceError(f"CSV not found: {filepath}")
    except Exception as e:
        raise DataSourceError(f"Error reading CSV: {e}")
@timing_decorator
def detect_and_read_csv(filepath: str) -> pd.DataFrame:
   
    with open(filepath, "rb") as f:
        raw = f.read(100000)  
        result = chardet.detect(raw)
        encoding = result["encoding"]
        confidence = result["confidence"]

    logger.info(f"Detected encoding: {encoding} (confidence: {confidence:.0%})")

    try:
        return pd.read_csv(filepath, encoding=encoding)
    except UnicodeDecodeError:
        logger.warning(f"Failed with {encoding}, falling back to latin-1")
        return pd.read_csv(filepath, encoding="latin-1")
@timing_decorator
def read_and_flatten_json(filepath: str) -> pd.DataFrame:
    
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        df = pd.json_normalize(data, sep=".")
        logger.info(f"Flattened JSON to {df.shape[0]} rows, {df.shape[1]} columns")
        return df

    except FileNotFoundError:
        raise DataSourceError(f"JSON file not found: {filepath}")
    except json.JSONDecodeError as e:
        raise DataSourceError(f"Invalid JSON: {e}")
@timing_decorator
def read_excel_sheets(filepath: str) -> pd.DataFrame:

    try:
       
        sheets = pd.read_excel(filepath, sheet_name=None)
        logger.info(f"Found {len(sheets)} sheets: {list(sheets.keys())}")

        dfs = []
        for sheet_name, df in sheets.items():
            df["sheet_name"] = sheet_name  

        merged = pd.concat(dfs, ignore_index=True)
        logger.info(f"Merged shape: {merged.shape}")
        return merged

    except FileNotFoundError:
        raise DataSourceError(f"Excel file not found: {filepath}")
    except Exception as e:
        raise DataSourceError(f"Error reading Excel: {e}")
@timing_decorator
def read_compressed_csv(filepath: str) -> pd.DataFrame:
    try:
        if filepath.endswith(".gz"):
            return pd.read_csv(filepath, compression="gzip")

        elif filepath.endswith(".zip"):
            with zipfile.ZipFile(filepath, "r") as z:
                csv_files = [f for f in z.namelist() if f.endswith(".csv")]
                if not csv_files:
                    raise DataSourceError("No CSV found inside zip file")

                with z.open(csv_files[0]) as f:
                    return pd.read_csv(io.BytesIO(f.read()))
        else:
            raise DataSourceError(f"Unsupported compression format: {filepath}")

    except FileNotFoundError:
        raise DataSourceError(f"Compressed file not found: {filepath}")
@timing_decorator
def parquet_to_csv(parquet_path: str,csv_output_path: str) -> pd.DataFrame:
    try:
        df = pd.read_parquet(parquet_path)
        logger.info(f"Parquet loaded: {df.shape}, dtypes: {df.dtypes.to_dict()}")

        df.to_csv(csv_output_path, index=False)
        logger.info(f"Saved to CSV: {csv_output_path}")
        return df

    except FileNotFoundError:
        raise DataSourceError(f"Parquet file not found: {parquet_path}")
    except Exception as e:
        raise DataSourceError(f"Error reading Parquet: {e}")
class NewFileHandler(FileSystemEventHandler):
    
    def on_created(self, event) -> None:
        if event.is_directory:
            return
        filepath = event.src_path
        logger.info(f"New file detected: {filepath}")
        self._process_file(filepath)

    def _process_file(self, filepath: str) -> None:
       
        if filepath.endswith(".csv"):
            logger.info(f"Processing CSV: {filepath}")
            for chunk in read_csv_chunked(filepath):
                logger.info(f"  chunk shape: {chunk.shape}")

        elif filepath.endswith(".json"):
            logger.info(f"Processing JSON: {filepath}")
            df = read_and_flatten_json(filepath)
            logger.info(f"  shape: {df.shape}")

        elif filepath.endswith(".parquet"):
            logger.info(f"Processing Parquet: {filepath}")
            df = pd.read_parquet(filepath)
            logger.info(f"  shape: {df.shape}")

        else:
            logger.warning(f"Unsupported file type: {filepath}")


def watch_directory(directory: str) -> None:
  
    handler = NewFileHandler()
    observer = Observer()
    observer.schedule(handler, path=directory, recursive=False)
    observer.start()
    logger.info(f"Watching directory: {directory}")

    try:
        while True:
            import time
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        logger.info("File watcher stopped")
    observer.join()  
# pyflow/extractors.py — add at the bottom

class BaseExtractor:
    """Base class for all extractors. Defines the interface."""

    def __init__(self, filepath: str) -> None:
        self.filepath = filepath

    def extract(self) -> pd.DataFrame:
        raise NotImplementedError("Subclasses must implement extract()")

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(filepath='{self.filepath}')"


class CSVExtractor(BaseExtractor):
    """Extracts data from CSV files in memory-safe chunks."""

    def __init__(self, filepath: str, chunk_size: int = 100000) -> None:
        super().__init__(filepath)
        self.chunk_size = chunk_size

    @timing_decorator
    def extract(self) -> pd.DataFrame:
        chunks = []
        for chunk in read_csv_chunked(self.filepath, self.chunk_size):
            chunks.append(chunk)
        df = pd.concat(chunks, ignore_index=True)
        logger.info(f"CSVExtractor: loaded {df.shape[0]} rows")
        return df


class JSONExtractor(BaseExtractor):
    """Extracts and flattens nested JSON files."""

    @timing_decorator
    def extract(self) -> pd.DataFrame:
        df = read_and_flatten_json(self.filepath)
        logger.info(f"JSONExtractor: loaded {df.shape[0]} rows")
        return df


class ParquetExtractor(BaseExtractor):
    """Extracts data from Parquet files."""

    @timing_decorator
    def extract(self) -> pd.DataFrame:
        try:
            df = pd.read_parquet(self.filepath)
            logger.info(f"ParquetExtractor: loaded {df.shape[0]} rows")
            return df
        except FileNotFoundError:
            raise DataSourceError(f"Parquet file not found: {self.filepath}")