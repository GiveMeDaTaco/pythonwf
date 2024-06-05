import re
from typing import Union, List, Set, Dict, Any


class ConstructSQLMeta(type):
    """
    Metaclass for validating the structure of the '_unique_identifiers' attribute and ensuring it adheres to specific
    formatting rules. Also includes methods for extracting table aliases and performing validations.
    """

    def __setattr__(cls, name: str, value: Any) -> None:
        """
        Overrides the __setattr__ method to include validation checks for specific attributes.

        Args:
            name (str): The name of the attribute.
            value (Any): The value of the attribute.

        Raises:
            ValueError: If the attribute value is invalid.
        """
        if name == '_unique_identifiers':
            if value is not None:
                cls.validate_unique_identifiers(value)
        super().__setattr__(cls, name, value)

    @staticmethod
    def _extract_table_aliases(tables: Dict[str, List[Dict[str, Any]]]) -> Set[str]:
        """
        Extracts table aliases from the provided tables dictionary.

        Args:
            tables (Dict[str, List[Dict[str, Any]]]): The tables from which to extract aliases.

        Returns:
            Set[str]: A set of table aliases.
        """
        table_aliases: Set[str] = set()
        for table in tables.get('tables', []):
            table_alias = table.get('alias')
            if table_alias not in table_aliases:
                table_aliases.add(table_alias)
        return table_aliases

    def validate_unique_identifiers(cls, value: Union[List[str], Set[str]]) -> None:
        """
        Validates the structure and format of the '_unique_identifiers' attribute.

        Args:
            value (Union[List[str], Set[str]]): The unique identifiers to validate.

        Raises:
            ValueError: If the structure or format is invalid.
        """
        if not isinstance(value, (list, set)):
            raise ValueError("_unique_identifiers must be a list or set of strings.")

        unique_identifiers: Set[str] = set(value) if isinstance(value, list) else value
        pattern = re.compile(r'^[a-z]+\.[a-zA-Z0-9_]+$')
        columns_seen: Dict[str, str] = {}

        table_aliases = cls._extract_table_aliases(cls._tables)

        for identifier in unique_identifiers:
            parts = [part.strip() for part in identifier.split(',')]
            for part in parts:
                if not pattern.match(part):
                    raise ValueError(f"Invalid identifier format: {part}")

                alias, column = part.split('.')
                if alias not in table_aliases:
                    raise ValueError(f"Alias '{alias}' not present in _table_aliases.")

                if column in columns_seen and columns_seen[column] != alias:
                    raise ValueError(f"Column '{column}' is used with multiple aliases.")

                columns_seen[column] = alias
