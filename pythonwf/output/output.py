from pythonwf.logging.logging import call_logger, CustomLogger
from pythonwf.eligibility.eligibility import Eligible
from pythonwf.connections.teradata import TeradataHandler
import pandas as pd
from datetime import datetime

from pythonwf.waterfall.waterfall import Waterfall


class Output:
    def __init__(
            self,
            conditions: dict,
            sqlconstructor,
            logger: CustomLogger,
            teradata_connection: TeradataHandler = None
    ):
        self.logger = logger
        self.conditions = conditions
        self.teradata_connection = teradata_connection
        self.sqlconstructor = sqlconstructor

        # initialize output queries
        self._output_instructions = None
        self._output_queries = {}

    @property
    def output_instructions(self):
        return self._output_instructions

    @output_instructions.setter
    def output_instructions(self, output_instructions: dict[str:dict[str:str]]) -> None:
        """
        File structure should be:
        {
            'channel_name': {
                'sql': 'PLACE SQL HERE',
                'file_location': '/path/to/file', <- does not end with a forward slash
                'file_base_name': 'base_name_without_file_extension',
                'output_options': {
                    'format': 'parquet', 'csv', 'excel' <- only three options
                    'additional_arguments': {...} <-- these must match the python arguments for pandas.to_csv,
                                                        pandas.to_parquet, pandas.to_excel
                }
            },
            ...
        }

        Make sure that your sql uses the FROM statement: "FROM eligibility_table" (use any alias for joins)
        For example:
            SELECT ...
            FROM eligibility_table a
            LEFT JOIN some_other_table b
                ON a.some_column = b.some_column

        :param output_instructions:
        :return:
        """
        self._output_instructions = output_instructions

    @classmethod
    def from_waterfall(cls, waterfall: Waterfall) -> 'Output':
        return Output(
            waterfall.conditions,
            waterfall._sqlconstructor,
            waterfall.logger,
            waterfall._teradata_connection
        )

    def _create_channel_eligiblity(self, channels):
        self.sqlconstructor.output_file.generate_output_queries(channels)

    @call_logger
    def create_output_file(self) -> None:
        """
        Output the file for the specified channel(s)
        """
        # extract just the queries from the channels
        for channel, details in self.output_instructions.items(): # TODO: add metaclass check to make sure output_instructions is not null on call of this function
            self._output_queries[channel] = details.get('sql')
        self.sqlconstructor.output_queries = self._output_queries
        queries: dict = self.sqlconstructor.output_file.generate_output_queries()

        self.logger.info(f'{self.__class__.__name__} output queries: {queries}')

        for channel, query in queries.items():
            if self.teradata_connection is not None:
                df = self.teradata_connection.fastexport(query)
                self._save_output_file(df, channel)

    @call_logger
    def _save_output_file(self, df: pd.DataFrame, channel: str) -> None:
        """

        :param df: dataframe to save
        :param channel: channel corresponding to the dataframe
        :return: None
        """
        file_location = self._output_instructions.get(channel).get('file_location')
        file_base_name = self._output_instructions.get(channel).get('file_base_name')
        file_extension = self._output_instructions.get(channel).get('output_options').get('format')
        file_name = f'{file_location}/{file_base_name}_{datetime.now().strftime("%Y%m%d_%H%M")}.{file_extension}'
        output_options = self._output_instructions.get(channel).get('output_options')

        if output_options.get('csv'):
            df.to_csv(file_name, index=False, **output_options.get('additional_arguments'))
        elif output_options.get('excel'):
            df.to_excel(file_name, index=False, **output_options.get('additional_arguments'))
        elif output_options.get('parquet'):
            df.to_parquet(file_name, index=False, **output_options.get('additional_arguments'))





