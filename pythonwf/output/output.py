from pythonwf.logging.logging import call_logger, CustomLogger
from pythonwf.eligibility.eligibility import Eligible
from pythonwf.connections.teradata import TeradataHandler


class Output:
    def __init__(
            self,
            conditions: dict,
            eligibility_table: str,
            sqlconstructor,
            logger: CustomLogger,
            teradata_connection: TeradataHandler = None
    ):
        self.logger = logger
        self.conditions = conditions
        self.eligibility_table = eligibility_table
        self.teradata_connection = teradata_connection
        self.sqlconstructor = sqlconstructor

    def _create_channel_eligiblity(self, channels):
        self.sqlconstructor.output_file.generate_output_queries(channels) # ended here
    def output_file(self, output_queries: dict):
        """
        Output the file for the specified channel(s)
        :param output_queries: channel(s) to output
            {'example_channel': 'SELECT some_columns, ... FROM {eligibility_table} ...'}
            'example_channel' must match the channel used in the eligibility conditions.
            The FROM table must be "{eligibility_table}"; you can perform any joins to this table. The keys would be one
            of the unique identifier columns
        :return:
        """
        channels = output_queries.keys()
        for channel, query in output_queries:
            query = query.format(eligibility_table=self.eligibility_table)




