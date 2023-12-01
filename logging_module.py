import logging
from logging.handlers import RotatingFileHandler


def setup_logging():
    logging.basicConfig(level=logging.DEBUG)

    # Create a file handler for both error and info messages
    log_file = "log_output.txt"
    file_handler = RotatingFileHandler(log_file, mode='w', encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)
