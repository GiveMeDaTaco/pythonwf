class EligibilitySQLConstructor:
    def __init__(self, conditions, tables, eligibility_table, unique_identifiers):
        self.conditions = conditions
        self.tables = tables
        self.eligibility_table = eligibility_table.get('eligibility')
        self.unique_identifiers = unique_identifiers
        self.work_tables = tables.get('work_tables'

        # prep properties
        self._eligibility_sql = None

    def generate_eligible_sql(self) -> dict:
        select_sql = []
        for channel, templates in self.conditions.items():
            for template, checks in templates.items():
                for check in checks:
                    check_sql = check.get('sql')
                    check_output = check.get('output')
                    check_desc = check.get('description')
                    check_column_name = check.get('column_name')

                    select_sql.append(f'CASE WHEN {check_sql} THEN 1 ELSE 0 END AS {check_column_name}')

        table_sql = []
        where_sql = []
        for table in self.tables:
            table_name = table.get('name')
            table_alias = table.get('alias')
            table_join_conditions = table.get('join_conditions')
            table_where_conditions = table.get('where_conditions')
            table_join_type = table.get('join_type')

            table_sql.append(f'\n{table_join_type} {table_name} AS {table_alias} ON {table_join_conditions}')
            where_sql.append(f' ({table_where_conditions}) ')

        select_sql = ',\n'.join(select_sql)
        table_sql = '\n'.join(table_sql)

        if where_sql:
            where_sql = '\nWHERE ' + '\nAND '.join(where_sql)
        else:
            where_sql = ''

        sql = f"""
        CREATE TABLE {self.eligibility_table} AS (
            {','.join(self.unique_identifiers.get('with_aliases'))},
            {','.join(select_sql)}
            {table_sql}
            {where_sql}
            ) WITH DATA PRIMARY INDEX prindx ({','.join(self.unique_identifiers)});
        """

        collect_statistics_sql = f'COLLECT STATISTICS INDEX prindx ON {self.eligibility_table};'

        queries = {'query': sql, 'collect_query': collect_statistics_sql, 'table_name': self.eligibility_table}

        self._elibility_sql = queries

        return queries

    def generate_work_table_sql(self) -> list:
        '''
        Generates the SQL that can be used to create the user work tables

        :return:
        '''

        queries = []
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

            queries.append({'query': query, 'collect_query': collect_query, 'table_name': table_name})

        return queries
