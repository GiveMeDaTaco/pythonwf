import logging
from functools import wraps
from pythonwf.construct_sql.construct_sql import SQLConstructor
from collections import OrderedDict


class EligibleMeta(type):
    """
    Metaclass for validating the structure of the '_conditions' and '_tables' attributes,
    and ensuring certain attributes are not empty or changed after initialization in the Eligible class.
    Also sets up a logger and provides a decorator for logging function execution.
    """

    def __setattr__(cls, name, value):
        if name in {'_conditions', '_tables', '_unique_identifiers'}:
            if name == "_conditions":
                cls.validate_conditions(value)
            elif name == "_tables":
                cls.validate_tables(value)
            elif name == '_unique_identifiers':
                cls.validate_unique_identifiers(value)
        elif name in {'_campaign_planner', '_lead', '_username', '_offer_code'}:
            if not value:
                raise ValueError(f"{name} cannot be empty.")
        elif name in {'_log_location', '_log_level'} and hasattr(cls, name):
            raise ValueError(f"{name} cannot be changed once set.")
        super().__setattr__(name, value)

    @staticmethod
    def log_validation_error(self, message):
        """
        Logs a validation error message to the class instance's logger.

        Args:
            self: The class instance.
            message (str): The error message to be logged.
        """
        if hasattr(self, 'logger'):
            self.logger.error(message)
        else:
            raise ValueError(message)  # Fallback in case logger isn't set up yet

    @staticmethod
    def validate_conditions(conditions):
        """
        Validates the structure of the '_conditions' attribute.

        The '_conditions' attribute must adhere to the following structure:
        {
            'main': {
                'BA': [
                    {'sql': 'some_sql', 'output': False, 'description': 'some description here'},
                    ...
                ],
                ...
            },
            'some_channel': { 'BA': [{'sql': 'some_sql', 'output': False, 'description': 'some description here'},
                    ...], 'segment1': [{'sql': 'some_sql', 'output': False, 'description': 'some description here'},
                    ...]}
        }

        Raises:
            ValueError: If the structure is not valid.
        """
        if not isinstance(conditions, OrderedDict):
            raise ValueError("Conditions must be a dictionary.")
        if 'main' not in conditions:
            raise ValueError("Conditions must contain a 'main' key.")

        for key, subdict in conditions.items():
            if not isinstance(subdict, OrderedDict):
                raise ValueError(f"Value for key '{key}' must be a dictionary.")

            if key == 'main':
                if list(subdict.keys()) != ['BA']:
                    raise ValueError("'main' can only have 'BA' as its key.")
                for subkey, sublist in subdict.items():
                    if subkey == 'BA':
                        for item in sublist:
                            if not isinstance(item, OrderedDict):
                                raise ValueError(f"Each item in the list for key 'main' -> 'BA' must be a dictionary.")
                            if item.get('output') is True:
                                raise ValueError("Items under 'main' -> 'BA' cannot have 'output: True'.")
                            required_keys = {'sql', 'output', 'description'}
                            if not required_keys.issubset(item.keys()):
                                raise ValueError(
                                    f"Each dictionary in the list for key 'main' -> 'BA' must contain keys: {required_keys}")
            else:
                output_true_count = 0
                for subkey, sublist in subdict.items():
                    if not isinstance(sublist, list):
                        raise ValueError(f"Value for key '{key}' -> '{subkey}' must be a list.")
                    for item in sublist:
                        if not isinstance(item, OrderedDict):
                            raise ValueError(
                                f"Each item in the list for key '{key}' -> '{subkey}' must be a dictionary.")
                        if item.get('output') is True:
                            output_true_count += 1
                        required_keys = {'sql', 'output', 'description'}
                        if not required_keys.issubset(item.keys()):
                            raise ValueError(
                                f"Each dictionary in the list for key '{key}' -> '{subkey}' must contain keys: {required_keys}")

                    if subkey != 'BA':
                        if output_true_count > 1:
                            raise ValueError(
                                f"Only one item in the list for any subkey under '{key}' can have 'output: True'.")
                        if output_true_count == 1 and not sublist[-1].get('output'):
                            raise ValueError(
                                f"The last item in the list for key '{key}' -> '{subkey}' must have 'output: True'.")

    @staticmethod
    def validate_tables(tables):
        """
        Validates the structure of the '_tables' attribute.

        The '_tables' attribute must adhere to the following structure:
        {
            'tables': [
                {'table_name': 'schema_name.table_name', 'join_type': 'valid join type', 'alias': 'alias', 'where_conditions': 'sql code', 'join_conditions': 'sql code'},
                ...
            ],
            'work_tables': [
                {'sql': 'sql code', 'join_type': 'valid join type', 'alias': 'alias', 'where_conditions': 'sql code', 'join_conditions': 'sql code'},
                ...
            ]
        }

        Raises:
            ValueError: If the structure is not valid.
        """
        valid_keys = {'tables', 'work_tables'}
        if not isinstance(tables, dict):
            raise ValueError("Tables must be a dictionary.")
        if not valid_keys.issuperset(tables.keys()):
            raise ValueError(f"Tables must contain only the following keys: {valid_keys}")

        from_count = 0

        for key, sublist in tables.items():
            if not isinstance(sublist, list):
                raise ValueError(f"Value for key '{key}' must be a list.")
            for i, item in enumerate(sublist):
                if not isinstance(item, dict):
                    raise ValueError(f"Each item in the list for key '{key}' must be a dictionary.")
                if key == 'tables':
                    required_keys = {'table_name', 'join_type', 'alias', 'where_conditions', 'join_conditions'}
                    if i == 0 and item.get('join_type') == 'FROM':
                        from_count += 1
                else:  # key == 'work_tables'
                    required_keys = {'sql', 'join_type', 'alias', 'where_conditions', 'join_conditions'}
                    if item.get('join_type') == 'FROM':
                        from_count += 1

                if not required_keys.issubset(item.keys()):
                    raise ValueError(f"Each dictionary in the list for key '{key}' must contain keys: {required_keys}")

        if from_count != 1:
            raise ValueError(
                "There must be exactly one 'FROM' join type between the first item in the 'tables' list and any item "
                "in the 'work_tables' list; any FROM in 'tables' must be the first in the list")

    @staticmethod
    def validate_unique_identifiers(unique_identifiers):
        """
        Validates the structure of the '_unique_identifiers' attribute.

        The '_unique_identifiers' attribute must be a list of strings.

        Raises:
            ValueError: If the structure is not valid.
        """
        if not isinstance(unique_identifiers, list):
            raise ValueError("Unique identifiers must be a list.")
        if not all(isinstance(item, str) for item in unique_identifiers):
            raise ValueError("All items in unique identifiers must be strings.")

    @staticmethod
    def validate_non_empty(self):
        """
        Ensures that certain attributes are not empty after initialization.

        Validates that the following attributes are not empty:
        - self._campaign_planner
        - self._lead
        - self._username
        - self._offer_code

        Logs an error if any of the attributes are empty and raises a ValueError.
        """
        non_empty_attributes = ['_campaign_planner', '_lead', '_username', '_offer_code']
        for attr in non_empty_attributes:
            if not getattr(self, attr):
                message = f"{attr} cannot be empty."
                self.log_validation_error(message)
                raise ValueError(message)

    @staticmethod
    def setup_logger(self):
        """
        Sets up a logger for the Eligible class instance.

        The logger will use:
        - self._log_location as the location for the log.
        - self._offer_code + '_eligibility.log' as the base name.
        - self._log_level as the log level.
        """
        logger = logging.getLogger(f"{self._offer_code}_eligibility")
        logger.setLevel(self._log_level)
        handler = logging.FileHandler(f"{self._log_location}/{self._offer_code}_eligibility.log")
        handler.setLevel(self._log_level)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        self.logger = logger

    @staticmethod
    def log_execution(func):
        """
        Decorator for logging the execution of a function.

        Logs an entry when the function starts and logs an error if the function raises an exception.

        Args:
            func (function): The function to be decorated.

        Returns:
            function: The wrapped function with logging.
        """

        @wraps(func)
        def wrapper(self, *args, **kwargs):
            self.logger.info(f"Running {func.__name__}")
            try:
                result = func(self, *args, **kwargs)
                return result
            except Exception as e:
                self.logger.error(f"There was an issue with {func.__name__}:\n\n\t\t{e}")
                raise

        return wrapper

    def __call__(cls, *args, **kwargs):
        instance = super().__call__(*args, **kwargs)
        cls.setup_logger(instance)
        cls.validate_non_empty(instance)
        return instance
