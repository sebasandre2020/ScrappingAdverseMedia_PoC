import logging
import random
import time
from trace import Trace
from utils.constants import USER_AGENTS

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def log_execution_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        elapsed_time = end_time - start_time
        logging.info(f"Execution time of {func.__name__}: {elapsed_time:.2f} seconds")
        return result
    return wrapper

def set_random_user_agent(headers: object):
    headers["User-Agent"] = random.choice(USER_AGENTS)
    return headers
