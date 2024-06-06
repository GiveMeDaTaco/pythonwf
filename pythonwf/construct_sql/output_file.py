from pythonwf.logging.logging import CustomLogger, call_logger

class OutputFileSQLConstructor:
    def __init__(
            self,
            output_queries,
            conditions,
            eligibility_table,
            logger
    ):
        self.logger = logger
        self.output_queries = output_queries
        self.conditions = conditions
        self.eligibility_table = eligibility_table

    def generate_base_eligible_sql(self):
        sql_statements = {}

        # Extract the WHERE conditions from 'main'
        where_conditions = []
        for template, checks in self.conditions.get('main', {}).items():
            for check in checks:
                where_conditions.append(check['column_name'] + ' = 1')

        # Generate CASE statements for each channel and template
        for channel, templates in self.conditions.items():
            if channel == 'main':
                continue

            case_statements = []
            for template, checks in templates.items():
                checks_conditions = " AND ".join([check['column_name'] + ' = 1' for check in checks])
                case_statement = f"WHEN {checks_conditions} THEN '{template}'"
                case_statements.append(case_statement)

            # Combine the CASE statements for each channel
            case_sql = "SELECT CASE " + " ".join(case_statements) + f" END AS template_id"
            where_sql = "WHERE " + " AND ".join(where_conditions)
            full_sql = f"{case_sql} FROM {self.eligibility_table} {where_sql};"
            sql_statements[channel] = full_sql

        return sql_statements

    def generate_output_sql(self):
        queries = {}
        base_tables = self.generate_base_eligible_sql()
        for channel, query in self.output_queries.items():
            channel_eligible = base_tables.get(channel)
            query = query.format(eligibility_table=channel_eligible)
            queries[channel] = query

        return queries


