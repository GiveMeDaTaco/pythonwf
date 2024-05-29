from pythonwf.logging.logging import CustomLogger

class WaterfallSQLConstructor:
    def __init__(
            self,
            conditions,
            conditions_column_mappings,
            backend_tables,
            parsed_unique_identifiers
    ):
        self.conditions = conditions
        self._backend_tables = backend_tables
        self.parsed_unique_identifiers = parsed_unique_identifiers
        self._conditions_column_mappings = conditions_column_mappings
        self._regain_sql = None
        self._incremental_drops_sql = None
        self._unique_drops_sql = None
        self._remaining_sql = None


    def generate_unique_identifier_details_sql(self):
        queries = {}

        column_names = []
        for channel, templates in self.conditions.items():
            for template, check in templates.items():
                column_names.append(check.get('column_name'))

        min_columns = [f'\nMIN({check}) AS min_{check}' for check in self.conditions]
        max_columns = [f'\nMAX({check}) AS max_{check}' for check in self.conditions]
        sum_columns = [f'\nSUM({check}) AS sum_{check}' for check in self.conditions]
        select_sql = min_columns + max_columns + sum_columns

        for identifier in self.parsed_unique_identifiers.get('original_without_aliases'):
            identifier_details_table = self._backend_tables.get(identifier)
            sql = f"""
                CREATE TABLE {identifier_details_table} AS (
                    SELECT
                        {identifier},
                        {','.join(select_sql)}
                    FROM
                        {self._backend_tables.get('eligibility')}) WITH DATA PRIMARY INDEX prindx ({identifier});
            """

            queries[identifier] = sql

        return queries

    def generate_unique_drops_sql(self):
        """
        Generates the SQL used to query the eligibility table for unique drops on each check

        :return:
        """
        queries = dict()

        for identifier in self.parsed_unique_identifiers.get('original_without_aliases'):
            case_statements = []

            for check in self._conditions_column_mappings.keys():
                case_statement = f"SUM(CASE WHEN max_{check} = 0 THEN 1 ELSE 0 END) AS {check}"
                case_statements.append(case_statement)

            query = 'SELECT\n'
            query += ',\n'.join(case_statements)
            query += f'\nFROM {self._backend_tables.get(identifier)};'

            queries[identifier] = query

        self._unique_drops_sql = queries
        return queries

    def generate_regain_sql(self) -> dict:
        """
        Generates the SQL used to query the eligibility table for the records regained if each check was removed
        :return:
        """
        queries = dict()

        for identifier in self.parsed_unique_identifiers.get('original_without_aliases'):
            case_statements = []

            for check, related_checks in self._conditions_column_mappings.items():
                conditions = []

                # Add condition for the selected check
                conditions.append(f'max_{check} = 0')

                # Add conditions for base checks
                conditions.extend([f'max_{base_check} = 1' for base_check in related_checks['base']])

                # Add conditions for prior templates checks
                prior_template_conditions = []
                for template, template_checks in related_checks['prior_templates'].items():
                    prior_template_condition = ' OR '.join(
                        [f'max_{prior_check} = 0' for prior_check in
                         template_checks['no_output'] + template_checks['output']]
                    )
                    if prior_template_condition:
                        prior_template_conditions.append(f'({prior_template_condition})')

                # Add conditions for post templates checks
                post_template_conditions = []
                for template, template_checks in related_checks['post_templates'].items():
                    post_template_condition = ' OR '.join(
                        [f'max_{post_check} = 0' for post_check in
                         template_checks['no_output'] + template_checks['output']]
                    )
                    if post_template_condition:
                        post_template_conditions.append(f'({post_template_condition})')

                # Combine conditions
                if prior_template_conditions:
                    conditions.append(' AND '.join(prior_template_conditions))
                if post_template_conditions:
                    conditions.append(' AND '.join(post_template_conditions))

                case_statement = ' AND '.join(conditions)
                case_statements.append(f"\nSUM(CASE WHEN {case_statement} THEN 1 ELSE 0 END) AS {check}")

            query = 'SELECT\n'
            query += ',\n'.join(case_statements)
            query += f'\nFROM {self._backend_tables.get(identifier)};'

            queries[identifier] = query

        self._regain_sql = queries
        return queries

    def generate_incremental_drops_sql(self):
        """
        Generates the SQL used to query the eligibility table for incremental drops on each check

        :return:
        """
        queries = dict()

        for identifier in self.parsed_unique_identifiers.get('original_without_aliases'):
            case_statements = []

            for check, related_checks in self._conditions_column_mappings.items():
                conditions = []

                # Add condition for the selected check
                conditions.append(f'max_{check} = 0')

                # Add conditions for base checks
                conditions.extend([f'max_{base_check} = 1' for base_check in related_checks['base']])

                # Add conditions for prior template checks
                prior_conditions = []
                for template, template_checks in related_checks['prior_templates'].items():
                    template_condition = ' OR '.join(
                        [f'max_{prior_check} = 0' for prior_check in
                         template_checks['no_output'] + template_checks['output']]
                    )
                    if template_condition:
                        prior_conditions.append(f'({template_condition})')

                if prior_conditions:
                    conditions.append(' AND '.join(prior_conditions))

                case_statement = ' AND '.join(conditions)
                case_statements.append(f"\nSUM(CASE WHEN {case_statement} THEN 1 ELSE 0 END) AS {check}")

            query = 'SELECT\n'
            query += ',\n'.join(case_statements)
            query += f'\nFROM {self._backend_tables.get(identifier)};'

            queries[identifier] = query

        self._incremental_drops_sql = queries
        return queries

    def generate_remaining_sql(self):
        queries = dict()

        for identifier in self.parsed_unique_identifiers.get('original_without_aliases'):
            case_statements = []
            previous_checks = []

            for check, related_checks in self._conditions_column_mappings.items():
                total_rows_statement = f"COUNT(*) AS total_rows"

                passing_checks = ' AND '.join([f'max_{col} = 1' for col in previous_checks + [check]])
                prior_failed_checks = ' OR '.join(
                    [f'max_{col} = 0' for template in related_checks['prior_templates'].values() for col in
                     template['no_output'] + template['output']])

                if prior_failed_checks:
                    prior_failed_checks = f' AND ({prior_failed_checks})'
                else:
                    prior_failed_checks = ''

                case_statement = f"total_rows - SUM(CASE WHEN {passing_checks} {prior_failed_checks} THEN 1 ELSE 0 END) AS remaining_{check}"
                case_statements.append(case_statement)
                previous_checks.append(check)

            query = 'SELECT\n'
            query += ',\n'.join([total_rows_statement] + case_statements)
            query += f'\nFROM {self._backend_tables.get(identifier)};'

            queries[identifier] = query

        self._remaining_sql = queries
        return queries
