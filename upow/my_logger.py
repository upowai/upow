# my_logger.py

import logging
import os
from logging.handlers import RotatingFileHandler


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class CustomLogger(metaclass=Singleton):
    def __init__(self, module_name: str, log_dir: str = 'logs', log_file: str = 'app.log',
                 max_bytes: int = 5 * 1024 * 1024,
                 backup_count: int = 100):
        """
        Initialize the custom logger.
        Args:
            - name (str): Logger name.
            - log_dir (str): Log directory path. Defaults to 'logs'.
            - log_file (str): Log file path. Defaults to 'app.log'.
            - max_bytes (int): Maximum log file size in bytes. Defaults to 10MB.
            - backup_count (int): Number of backup log files. Defaults to 30.
        """
        self.logger = logging.getLogger('upow')
        self.logger.setLevel(logging.DEBUG)  # Set the logging level

        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        log_file_path = os.path.join(log_dir, log_file)

        file_handler = RotatingFileHandler(log_file_path, maxBytes=max_bytes, backupCount=backup_count)
        file_handler.setLevel(logging.DEBUG)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s - %(filename)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        self.logger.module_name = module_name

    def get_logger(self):
        return self.logger
