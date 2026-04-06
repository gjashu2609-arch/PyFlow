
import logging
import logging.handlers
import os


def setup_logger(
    name: str       = "pyflow",
    log_level: str  = "INFO",
    log_dir: str    = "logs"
) -> logging.Logger:
    
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    formatter_simple = logging.Formatter(
        "%(asctime)s — %(levelname)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    formatter_detailed = logging.Formatter(
        "%(asctime)s — %(name)s — %(levelname)s — %(funcName)s:%(lineno)d — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    console = logging.StreamHandler()
    console.setLevel(getattr(logging, log_level))
    console.setFormatter(formatter_simple)

   
    file_handler = logging.handlers.RotatingFileHandler(
        filename    = f"{log_dir}/pyflow.log",
        maxBytes    = 5 * 1024 * 1024,
        backupCount = 3,
        encoding    = "utf-8"
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter_detailed)

    error_handler = logging.handlers.RotatingFileHandler(
        filename    = f"{log_dir}/pyflow_errors.log",
        maxBytes    = 2 * 1024 * 1024,
        backupCount = 2,
        encoding    = "utf-8"
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter_detailed)

    if not logger.handlers:
        logger.addHandler(console)
        logger.addHandler(file_handler)
        logger.addHandler(error_handler)

    return logger


if __name__ == "__main__":
    logger = setup_logger("pyflow", log_level="DEBUG")

    logger.debug("This is a DEBUG message — only in file")
    logger.info("This is INFO — in console and file")
    logger.warning("This is a WARNING")
    logger.error("This is an ERROR — in all three handlers")

    print("\nCheck logs/ folder for pyflow.log and pyflow_errors.log")
