import re


class ConstructSQLMeta(type):
    def __setattr__(cls, name, value):
        if name == '_unique_identifiers':
            if value is not None:
                cls.validate_unique_identifiers(value)
        super().__setattr__(cls, name, value)

    @staticmethod
    def _extract_table_aliases(tables):
        table_aliases = set()
        for table in tables.get('tables'):
            table_alias = table.get('alias')
            if table_alias not in table_aliases:
                table_aliases.add(table_alias)
        return table_aliases

    def validate_unique_identifiers(cls, value):

        if not isinstance(value, (list, set)):
            raise ValueError("_unique_identifiers must be a list or set of strings.")

        unique_identifiers = set(value) if isinstance(value, list) else value

        pattern = re.compile(r'^[a-z]+\.[a-zA-Z0-9_]+$')
        columns_seen = {}

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

