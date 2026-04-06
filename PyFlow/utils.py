from PyFlow.exceptions_custom import DataSourceError
import json
import yaml
import time
import random
from PyFlow.exceptions_custom import LoadError
import logging
import functools
from collections import Counter
from contextlib import contextmanager
import sqlite3
def load_config(file_path):
    try :
        with open(file_path,'r') as f:
            if file_path.endswith((".yaml",'.yml')):
                return yaml.safe_load(f)
            elif file_path.endswith(('.json')):
                return  json.load(f)
            else :
                raise DataSourceError(f"unsupoorted formatt")
    except FileNotFoundError:
        raise DataSourceError(f'config not found')
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)
logger = logging.getLogger(__name__)
def timing_decorator(func):

    @functools.wraps(func)   
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        
        try:
            result = func(*args, **kwargs)
            end = time.perf_counter()
            logger.info(f"{func.__name__} completed in {end - start:.4f} seconds")
            return result
        except Exception as e:
            end = time.perf_counter()
            logger.error(f"{func.__name__} failed after {end - start:.4f} seconds — {e}")
            raise 
    return wrapper



def extract_email_domains(records: list[dict], email_field: str = "email") -> Counter:
    
    domains = [
        record[email_field].split("@")[1].lower()
        for record in records
        if email_field in record and "@" in record.get(email_field, "")
    ]

   
    return Counter(domains)


def get_top_domains(records: list[dict], n: int = 10, email_field: str = "email") -> dict:
   
    domain_counts = extract_email_domains(records, email_field)
    return {domain: count for domain, count in domain_counts.most_common(n)}

@contextmanager
def db_connection(db_path: str):
    
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        logger.info(f"Database connection opened: {db_path}")

        yield conn           

        conn.commit()             
        logger.info("Transaction committed successfully")

    except Exception as e:
        if conn:
            conn.rollback()        
            logger.error(f"Transaction rolled back due to: {e}")
        raise LoadError(f"Database operation failed: {e}")

    finally:
        if conn:
            conn.close()      
            logger.info("Database connection closed")

def retry_with_backoff(
    func,
    max_retries: int   = 3,
    base_delay:  float = 1.0,
    max_delay:   float = 60.0
):

    logger = logging.getLogger(__name__)
    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Attempt {attempt}/{max_retries}")
            return func()

        except Exception as e:
            last_error = e
            if attempt == max_retries:
                break

            
            delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
            jitter = random.uniform(0, delay * 0.1)
            wait = delay + jitter

            logger.warning(
                f"Attempt {attempt} failed: {e}. "
                f"Retrying in {wait:.1f}s..."
            )
            time.sleep(wait)

    raise LoadError(
        f"All {max_retries} attempts failed. Last error: {last_error}"
    )