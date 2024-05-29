from pythonwf.construct_sql.construct_sql import SQLConstructor
from pythonwf.connections.teradata import TeradataHandler

import pandas as pd
from collections import OrderedDict


class Waterfall:
    def __init__(
            self,
            conditions,
            offer_code,
            campaign_planner,
            lead,

            sql_constructor: SQLConstructor,
            teradata_connection: TeradataHandler
    ):
        self._sqlconstructor = SQLConstructor
        self._teradata_connection = teradata_connection

        self.conditions = conditions

        # prep column names
        self._column_names = OrderedDict({
            'unique_drops': '{identifier} drop if only this drop',
            'increm_drops': '{identifier} drop increm',
            'cumul_drops': '{identifier} drop cumul',
            'regain': '{identifier} regain if no scrub',
            'remaining': '{identifier} remaining'
        })

        # initializing properties
        self._query_results = dict()
        self._compiled_dataframes = OrderedDict()
        self._starting_population = None
        self._combined_df = None

    @property
    def conditions(self):
        return self._conditions

    @conditions.setter
    def conditions(self, value):
        value = self._prepare_conditions(value)
        self._conditions = value

    @staticmethod
    def _prepare_conditions(conditions):
        rows = []

        for channel, templates in conditions.items():
            for template, checks in templates.items():
                for check in checks:
                    column_name = check.get('column_name', None)
                    description = check.get('description', None)
                    sql = check.get('sql', None)

                    if column_name is not None:
                        modified_description = f'[{template}] {description}' if description else None
                        rows.append({'column_name': column_name, 'description': modified_description, 'sql': sql})

        df = pd.DataFrame(rows)
        return df


    def _save_results(self, identifier, data):
        if self._query_results.get(identifier) is None:
            self._query_results[identifier] = list()
        self._query_results[identifier].append(data)

    def _calculate_regain(self):
        queries = self._sqlconstructor.waterfall.generate_regain_sql()
        for identifier, query in queries:
            df = self._teradata_connection.to_pandas(query)
            df['Index'] = self._column_names.get('regain')
            df = df.set_index('Index')
            self._save_results(identifier, df)

    def _calculate_incremental_drops(self):
        queries = self._sqlconstructor.waterfall.generate_incremental_drops_sql()
        for identifier, query in queries:
            df = self._teradata_connection.to_pandas(query)
            df['Index'] = self._column_names.get('increm_drops')
            df = df.set_index('Index')
            self._save_results(identifier, df)

    def _calculate_unique_drops(self):
        queries = self._sqlconstructor.waterfall.generate_unique_drops_sql()
        for identifier, query in queries:
            df = self._teradata_connection.to_pandas(query)
            df['Index'] = self._column_names.get('unique_drops')
            df = df.set_index('Index')
            self._save_results(identifier, df)

    def _calculate_remaining(self):
        queries = self._sqlconstructor.waterfall.generate_remaining_sql
        for identifier, query in queries:
            df = self._teradata_connection.to_pandas(query)
            df['Index'] = self._column_names.get('remaining')
            df = df.set_index('Index')
            self._save_results(identifier, df)

    def step1_analyze_eligibility(self):

        # NOTE: the order matters here, as that determines what order of the list in _query_results
        self._calculate_unique_drops()
        self._calculate_incremental_drops()
        self._calculate_regain()
        self._calculate_remaining()

    def step2_create_dataframes(self):
        for identifier, dfs in self._query_results:
            df = pd.concat(dfs, axis=1).transpose()

            unique_drop = self._column_names.get('unique_drops').format(identifier=identifier)
            increm_drop = self._column_names.get('increm_drops').format(identifier=identifier)
            cumul_drop = self._column_names.get('cumul_drops').format(identifier=identifier)
            regain = self._column_names.get('regain').format(identifier=identifier)
            remaining = self._column_names.get('remaining').format(identifier=identifier)

            self._starting_population = df.loc[0, increm_drop] + df.loc[0, remaining]
            df[cumul_drop] = self._starting_population - df[remaining]

            df = df[unique_drop, increm_drop, cumul_drop, regain, remaining]
            self._compiled_dataframes[identifier] = df

    def step3_generate_report(self):
        # Ensure the 'column_name' column in self._conditions is a string for compatibility
        self._conditions['column_name'] = self._conditions['column_name'].astype(str)

        # Loop through each dataframe in self._compiled_dataframes and merge with self._conditions
        for identifier, df in self._compiled_dataframes.items():
            # Reset index of the dataframe to prepare for merge
            df_reset = df.reset_index()

            # Rename the 'Index' column to 'column_name'
            df_reset = df_reset.rename(columns={'Index': 'column_name'})

            # Merge with the conditions dataframe
            merged_df = pd.merge(df_reset, self._conditions, on='column_name', how='left')

            # Save the merged dataframe back to the compiled dataframes
            self._compiled_dataframes[identifier] = merged_df

        # Combine all the merged dataframes into a single report
        final_report = pd.concat(self._compiled_dataframes.values(), keys=self._compiled_dataframes.keys())

        # You can also return the final report or save it as needed
        self._combined_df = final_report




