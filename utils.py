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


def load_env_variables(path='./env.yaml'):
    '''
    Placeholder function to load environment variables from a yaml file 
    (In production, this would be replaced with a more secure method)
    '''
    with open(path) as f:
        env_vars = yaml.load(f, Loader=yaml.FullLoader)
    os.environ['AWS_ACCESS_KEY_ID'] = env_vars['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = env_vars['AWS_SECRET_ACCESS_KEY']
