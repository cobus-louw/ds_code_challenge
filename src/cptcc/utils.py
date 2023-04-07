from functools import wraps
import time
import yaml
import os

import logging
logger = logging.getLogger(__name__)


def timeit(func):
    '''
    A decorator to time a function
    '''
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        logger.info(f'Function {func.__name__} Took {total_time:.4f} seconds')
        return result
    return timeit_wrapper
