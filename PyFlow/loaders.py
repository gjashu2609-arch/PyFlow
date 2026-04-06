import pandas as pd
import logging
from PyFlow.utils import timing_decorator, db_connection
from PyFlow.exceptions_custom import LoadError

logger = logging.getLogger(__name__)


class DataLoader:
    def __init__(self, db_path: str, batch_size: int = 1000) -> None:
        self.db_path   = db_path
        self.batch_size = batch_size

    @timing_decorator
    def create_table(self, df: pd.DataFrame, table_name: str) -> None:
        type_map = {
            "int64":   "INTEGER",
            "int32":   "INTEGER",
            "int16":   "INTEGER",
            "float64": "REAL",
            "float32": "REAL",
            "object":  "TEXT",
            "bool":    "INTEGER",
        }

        cols = []
        for col, dtype in df.dtypes.items():
            sql_type = type_map.get(str(dtype), "TEXT")
            cols.append(f'"{col}" {sql_type}')

        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {", ".join(cols)}
            )
        """

        with db_connection(self.db_path) as conn:
            conn.execute(create_sql)
        logger.info(f"Table '{table_name}' ready")

    @timing_decorator
    def load(self, df: pd.DataFrame, table_name: str) -> int:

        self.create_table(df, table_name)

        total_inserted = 0
        df = df.copy()
        for col in df.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns:
            df[col] = df[col].astype(str)
        rows = df.to_dict(orient="records")

        batches = [
            rows[i:i + self.batch_size]
            for i in range(0, len(rows), self.batch_size)
        ]

        logger.info(f"Loading {len(rows)} rows in {len(batches)} batches")

        for batch_num, batch in enumerate(batches, 1):
            placeholders = ", ".join(["?" for _ in batch[0]])
            columns      = ", ".join([f'"{k}"' for k in batch[0].keys()])
            insert_sql   = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            values       = [list(row.values()) for row in batch]

            with db_connection(self.db_path) as conn:
                conn.executemany(insert_sql, values)
                total_inserted += len(batch)
                logger.info(f"  Batch {batch_num}/{len(batches)}: {len(batch)} rows inserted")

        logger.info(f"Load complete: {total_inserted} total rows")
        return total_inserted