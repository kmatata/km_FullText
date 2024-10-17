import os
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta
import rollbar
from functools import wraps
from dotenv import load_dotenv
import pytz

# Load environment variables
dotenv_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path=dotenv_path)
# Configure Rollbar
rollbar.init(
    os.getenv("ROLLBAR_ACCESS_TOKEN"), environment=os.getenv("ROLL_ENVIRONMENT")
)

LOG_DIR = os.getenv("LOG_DIR")
MAX_LOG_SIZE = 1 * 1024 * 1024  # 1 MB
BACKUP_COUNT = 5
MAX_LOG_AGE_HOURS = 2
eat_tz = pytz.timezone('Africa/Nairobi')
now = datetime.now(eat_tz)
class Logger:
    def __init__(self, name):
        self.logger = logging.getLogger(name)
        self.setup_logging()

    def setup_logging(self):
        if not os.path.exists(LOG_DIR):
            os.makedirs(LOG_DIR)
        
        log_file = os.path.join(LOG_DIR, f"{self.logger.name}_{now.strftime('%Y-%m-%d')}.log")
        file_handler = RotatingFileHandler(log_file, maxBytes=MAX_LOG_SIZE, backupCount=BACKUP_COUNT)
        # now.strftime("%Y-%m-%d %H:%M:%S")
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(file_handler)

    def debug(self, message):
        self.logger.debug(f"\n{message}\n")

    def info(self, message):
        self.logger.info(f"\n{message}\n")

    def warning(self, message):
        self.logger.warning(f"\n{message}\n")

    def error(self, message, exc_info=False):
        self.logger.error(f"\n{message}\n", exc_info=exc_info)
        rollbar.report_message(message, 'error')

    def critical(self, message, exc_info=True):
        self.logger.critical(f"\n{message}\n", exc_info=exc_info)
        rollbar.report_message(message, 'critical')

    @staticmethod
    def clear_old_logs():
        current_time = datetime.now()
        for filename in os.listdir(LOG_DIR):
            file_path = os.path.join(LOG_DIR, filename)
            file_modified = datetime.fromtimestamp(os.path.getmtime(file_path))
            if current_time - file_modified > timedelta(hours=MAX_LOG_AGE_HOURS):
                print("removing old logs")
                os.remove(file_path)

def log_exceptions(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        logger = Logger(func.__name__)
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Exception in {func.__name__}: {str(e)}", exc_info=True)
            raise
    return wrapper