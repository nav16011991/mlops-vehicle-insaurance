import logging
import os
from logging.handlers import RotatingFileHandler
from from_root import from_root
from datetime import datetime

LOG_DIR = 'logs'
LOG_FILE = f"log_{datetime.now().strftime('%Y-%m-%d')}.log"
MAX_LOG_FILE_SIZE = 10 * 1024 * 1024  # 10 MB
BACKUP_COUNT = 5

log_dir_path = os.path.join(from_root(), LOG_DIR)
os.makedirs(log_dir_path, exist_ok=True)
log_file_path = os.path.join(log_dir_path, LOG_FILE)

def configure_logger():
    """
    Configures and returns a logger that logs messages to both a rotating file and the console.
    """

    # Create custom logger
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # Define formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # File handler with rotation
    file_handler = RotatingFileHandler(
        log_file_path, maxBytes=MAX_LOG_FILE_SIZE, backupCount=BACKUP_COUNT
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)

    # Add handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)


# Configure the logger
configure_logger()