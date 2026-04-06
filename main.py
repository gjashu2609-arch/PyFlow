import logging
import logging.handlers
import sys
from PyFlow.utils import load_config, timing_decorator
from PyFlow.extractors import CSVExtractor, JSONExtractor,ParquetExtractor
from PyFlow.transformers import DataTransformer,export_dataframe
from PyFlow.loaders import DataLoader
from PyFlow.logger import setup_logger
from PyFlow.exceptions_custom import PyFlowError, DataSourceError, TransformationError, LoadError


def setup_logging(log_level: str = "INFO") -> None:
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, log_level))

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter(
        "%(asctime)s — %(levelname)s — %(message)s"
    ))

   
    file_handler = logging.handlers.RotatingFileHandler(
        "logs/pyflow.log",
        maxBytes=5 * 1024 * 1024,
        backupCount=3
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s — %(name)s — %(levelname)s — %(message)s"
    ))

    logger.addHandler(console)
    logger.addHandler(file_handler)


@timing_decorator
def run_pipeline(config_path: str = "config/config.yaml") -> None:
    
    logger = logging.getLogger(__name__)

    logger.info("=" * 50)
    logger.info("PyFlow ETL Pipeline starting")
    logger.info("=" * 50)

    config = load_config(config_path)
    logger.info(f"Config loaded from: {config_path}")

    logger.info("STEP 1: Extract")
    try:
        file_path = config["file_paths"]["csv"]
        extractor = CSVExtractor(
            filepath   = file_path,
            chunk_size = config["etl_settings"]["chunk_size"]
        )
        df = extractor.extract()
        logger.info(f"Extracted {df.shape[0]} rows from {file_path}")

    except DataSourceError as e:
        logger.error(f"Extraction failed: {e}")
        sys.exit(1)


    logger.info("STEP 2: Transform")
    try:
        transformer = DataTransformer(config)
        df = transformer.transform(df)
        logger.info(f"Transformation complete: {df.shape[0]} rows")

    except TransformationError as e:
        logger.error(f"Transformation failed: {e}")
        sys.exit(1)

    
    logger.info("STEP 3: Load")
    try:
        loader = DataLoader(
            db_path    = config["database"]["path"],
            batch_size = config["etl_settings"]["batch_size"]
        )
        total = loader.load(df, config["database"]["table_name"])
        logger.info(f"Loaded {total} rows to database")

    except LoadError as e:
        logger.error(f"Load failed: {e}")
        sys.exit(1)
    logger.info("STEP 4: Export")
    paths = export_dataframe(df, config["etl_settings"]["output_dir"])
    for fmt, path in paths.items():
        logger.info(f"  Exported {fmt}: {path}")

    logger.info("=" * 50)
    logger.info("PyFlow pipeline completed successfully")
    logger.info("=" * 50)

if __name__ == "__main__":
    import os
    os.makedirs("logs", exist_ok=True)
    setup_logger("pyflow", log_level="INFO")
    run_pipeline("config/config.yaml")