import functools
import sys
import logging


class DuplicateFilter(logging.Filter):
    def __init__(self):
        super().__init__()
        self.last_log = None

    def filter(self, record):
        current_log = (record.levelno, record.msg)
        if current_log == self.last_log:
            return False
        self.last_log = current_log
        return True


# Global variable to track indentation level
INDENT_LEVEL = 0


class CustomLogger:
    def __init__(self, name, level=logging.INFO, log_file=None, log_format=None, date_format=None):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

        # Set default log format and date format if not provided
        if log_format is None:
            log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        if date_format is None:
            date_format = '%Y-%m-%d %H:%M:%S'

        formatter = logging.Formatter(log_format, datefmt=date_format)

        # Create console handler and set level to debug
        ch = logging.StreamHandler()
        ch.setLevel(level)
        ch.setFormatter(formatter)
        ch.addFilter(DuplicateFilter())
        self.logger.addHandler(ch)

        # Create file handler if log_file is specified
        if log_file is not None:
            fh = logging.FileHandler(log_file)
            fh.setLevel(level)
            fh.setFormatter(formatter)
            fh.addFilter(DuplicateFilter())
            self.logger.addHandler(fh)

    def __getattr__(self, attr):
        return getattr(self.logger, attr)

    def indent_log(self, message):
        global INDENT_LEVEL
        indent = ' ' * (INDENT_LEVEL * 4)
        return f"{indent}{message}"

    def info(self, message, *args, **kwargs):
        message = self.indent_log(message)
        self.logger.info(message, *args, **kwargs)

    def error(self, message, *args, **kwargs):
        message = self.indent_log(message)
        self.logger.error(message, *args, **kwargs)


def call_logger(*var_names):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            global INDENT_LEVEL

            class_name = self.__class__.__name__
            function_name = func.__name__

            self.logger.info(f"Initiating {class_name}.{function_name}")
            INDENT_LEVEL += 1

            def trace_function(frame, event, arg):
                if event == "line":
                    if var_names:
                        for var_name in var_names:
                            if var_name in frame.f_locals:
                                self.logger.info(f"{class_name}.{function_name}: {var_name}=\n\t{frame.f_locals[var_name]}".replace('\n', '\n\t\t'))
                return trace_function

            try:
                if var_names:
                    sys.settrace(trace_function)
                result = func(self, *args, **kwargs)
                sys.settrace(None)

                INDENT_LEVEL -= 1
                self.logger.info(f"Finished {class_name}.{function_name}")
                return result
            except Exception as e:
                INDENT_LEVEL -= 1
                self.logger.error(f"Error in {class_name}.{function_name}: {e}")
                sys.settrace(None)
                raise

        return wrapper

    return decorator