from typing import Dict, List, Any, Optional
from pythonwf.logging.logging import call_logger, CustomLogger


class EligibilitySQLConstructor:
    """
    A class to generate SQL queries for the Eligibility process in eligibility checks.

    Attributes:
        conditions (Dict[str, Dict[str, Any]]): Conditions for eligibility checks.
        tables (Dict[str, List[Dict[str, Any]]]): Tables involved in the eligibility process.
        eligibility_table (str): The eligibility table.
        unique_identifiers (Dict[str, List[str]]): Unique identifiers used in the process.
        work_tables (List[Dict[str, Any]]): Work tables involved in the eligibility process.
        _eligibility_sql (Optional[Dict[str, str]]): SQL queries for eligibility.
    """

    def __init__(self, conditions: Dict[str, Dict[str, Any]], tables: List[Dict[str, Any]],
                 work_tables: List[Dict[str, Any]],
                 eligibility_table: Dict[str, str], unique_identifiers: Dict[str, List[str]], logger: CustomLogger) -> None:
        """
        Initializes the EligibilitySQLConstructor class with the provided parameters.

        Args:
            conditions (Dict[str, Dict[str, Any]]): Conditions for eligibility checks.
            tables (Dict[str, List[Dict[str, Any]]]): Tables involved in the eligibility process.
            eligibility_table (Dict[str, str]): The eligibility table.
            unique_identifiers (Dict[str, List[str]]): Unique identifiers used in the process.
        """
        self.logger = logger

        self.conditions = conditions
        self.tables = tables
        self.eligibility_table = eligibility_table
        self.unique_identifiers = unique_identifiers
        self.work_tables = work_tables

        # prep properties
        self._eligibility_sql = None

    @call_logger()
    def generate_eligible_sql(self) -> Dict[str, Any]:
        """
        Generates the SQL used to create the eligibility table with the necessary checks.

        Returns:
            Dict[str, Any]: A dictionary containing the main SQL query, collect statistics query, and table name.
        """
        select_sql: List[str] = []
        for channel, templates in self.conditions.items():
            for template, checks in templates.items():
                for check in checks:
                    check_sql = check.get('sql')
                    check_output = check.get('output')
                    check_desc = check.get('description')
                    check_column_name = check.get('column_name')

                    select_sql.append(f'CASE WHEN {check_sql} THEN 1 ELSE 0 END AS {check_column_name}')

        table_sql: List[str] = []
        where_sql: List[str] = []
        for table in self.tables:
            table_name = table.get('table_name')  # Ensure correct key usage
            table_alias = table.get('alias')
            table_join_conditions = table.get('join_conditions')
            table_where_conditions = table.get('where_conditions')
            table_join_type = table.get('join_type')

            join_condition_sql = f' ON {table_join_conditions}' if table_join_conditions else ''
            table_sql.append(f'\n{table_join_type} {table_name} AS {table_alias}{join_condition_sql}')
            where_sql.append(f' ({table_where_conditions}) ') if table_where_conditions else None

        select_sql_str = ',\n'.join(select_sql)
        table_sql_str = '\n'.join(table_sql)

        where_sql_str = '\nWHERE ' + '\nAND '.join(where_sql) if where_sql else ''

        sql = f"""
        CREATE TABLE {self.eligibility_table} AS (
            {','.join(self.unique_identifiers.get('with_aliases'))},
            {select_sql_str}
            {table_sql_str}
            {where_sql_str}
        ) WITH DATA PRIMARY INDEX prindx ({','.join(self.unique_identifiers.get('with_aliases'))});
        """

        collect_statistics_sql = f'COLLECT STATISTICS INDEX prindx ON {self.eligibility_table};'

        queries = {
            'query': sql,
            'collect_query': collect_statistics_sql,
            'table_name': self.eligibility_table
        }

        self._eligibility_sql = queries

        return queries

    @call_logger()
    def generate_work_table_sql(self) -> List[Dict[str, Any]]:
        """
        Generates the SQL used to create the user work tables.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries containing SQL queries for work tables.
        """
        queries: List[Dict[str, Any]] = []
        for table in self.work_tables:
            sql = table.get('sql')
            table_name = table.get('table_name')
            unique_index = table.get('unique_index')

            query = f'''
                CREATE TABLE {table_name} AS (
                    {sql}
                ) WITH DATA
            '''

            if unique_index is not None:
                query += f" PRIMARY INDEX prindx ({unique_index})"
                collect_query = f'COLLECT STATISTICS INDEX prindx ON {table_name}'
            else:
                collect_query = ''

            queries.append({
                'query': query,
                'collect_query': collect_query,
                'table_name': table_name
            })

        return queries
