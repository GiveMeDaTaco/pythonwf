import logging
from functools import wraps


class LoggerManager:
    def __init__(self, log_file='pipeline.log'):
        self.logger = logging.getLogger('pipeline_logger')
        self.logger.setLevel(logging.DEBUG)

        # Create a file handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)

        # Create a logging format
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)

        # Add the file handler to the logger
        self.logger.addHandler(file_handler)

    def log_decorator(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            class_name = args[0].__class__.__name__
            function_name = func.__name__
            self.logger.info(f"Initiating {class_name}.{function_name}")
            try:
                result = func(*args, **kwargs)
                self.logger.info(f"Finishing {class_name}.{function_name}")
                return result
            except Exception as e:
                self.logger.error(f"Error in {class_name}.{function_name}: {e}")
                raise
        return wrapper
