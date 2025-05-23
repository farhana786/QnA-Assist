import logging
import json
import sys
from datetime import datetime

WRITE_TO_FILE = False

class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_message = {
            "timestamp": datetime.now().isoformat(),
            "threadName": record.threadName,
            "filename": record.filename,
            "line": record.lineno,
            "function": record.funcName,
            "level": record.levelname,
            "message": record.getMessage()
        }
        return json.dumps(log_message)

def setup_logger(name="ApplicationLogger"):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Clear any existing handlers
    logger.handlers = []

    # Create a logging handler that writes log messages to stdout
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)

    if WRITE_TO_FILE:
        file_handler = logging.FileHandler("application_logs.json")
        file_handler.setFormatter(JsonFormatter())
        logger.addHandler(file_handler)
    
    return logger