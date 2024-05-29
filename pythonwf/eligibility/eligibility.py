from pythonwf.validations.eligibility import EligibleMeta
from pythonwf.connections.teradata import TeradataHandler
from pythonwf.construct_sql.construct_sql import SQLConstructor
from collections import OrderedDict


class Eligible(metaclass=EligibleMeta):
    def __init__(
            self,
            campaign_planner: str,
            lead: str,
            username: str,
            offer_code: str,
            conditions: OrderedDict,
            tables,
            unique_identifiers,
            teradata_connection: TeradataHandler,
            logger: LoggerManager
    ):
        self._campaign_planner = campaign_planner
        self._lead = lead
        self._username = username
        self._offer_code = offer_code
        self.logger_manager = logger

        # this will trigger the @setters for each of these variables below
        self.conditions = conditions
        self.tables = tables
        self.unique_identifiers = unique_identifiers

        # prep SQLConstructor property
        self._sqlconstructor = SQLConstructor(self.conditions, self.tables, self.unique_identifiers, self.username)
        self._teradata_connection = teradata_connection

    @property
    def campaign_planner(self):
        return self._campaign_planner

    @property
    def lead(self):
        return self._lead

    @property
    def username(self):
        return self._username

    @property
    def offer_code(self):
        return self._offer_code

    @property
    def conditions(self):
        return self._conditions

    @conditions.setter
    def conditions(self, conditions):
        self._conditions = conditions

    @property
    def tables(self):
        return self._tables

    @tables.setter
    def tables(self, tables):
        self._tables = tables

    @property
    def unique_identifiers(self):
        return self._unique_identifiers

    @unique_identifiers.setter
    def unique_identifiers(self, unique_identifiers):
        self._unique_identifiers = unique_identifiers

    def _create_work_tables(self):
        work_queries = self._sqlconstructor.eligible.generate_work_table_sql()

        for query in work_queries:
            sql = query.get('query')
            collect_sql = query.get('collect_query')
            table_name = query.get('table_name')

            if sql:
                self._teradata_connection.execute_query(sql)
                self._teradata_connection.tracking.track_table(table_name)
            if collect_sql:
                self._teradata_connection.execute_query(collect_sql)

    def generate_eligibility(self):
        self._create_work_tables()
        eligibility_query = self._sqlconstructor.eligible.generate_eligibility_sql()
        sql = eligibility_query.get('query')
        collect_sql = eligibility_query.get('collect_query')
        table_name = eligibility_query.get('table_name')

        self._teradata_connection.execute_query(sql)
        self._teradata_connection.tracking.track_table(table_name)
        self._teradata_connection.execute_query(collect_sql)


