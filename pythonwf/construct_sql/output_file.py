class OutputFileSQLConstructor:
    def __init__(
            self,
            output_columns,
            conditions,
            tables,
            eligibility_table,
            file_location
    ):
        self.output_columns = output_columns
        self.conditions = conditions
        self.tables = tables
        self.eligibility_table = eligibility_table

    def generate_output_file_sql(self):
        ...