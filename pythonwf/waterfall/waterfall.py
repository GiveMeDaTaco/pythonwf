from pythonwf.construct_sql.construct_sql import SQLConstructor
from pythonwf.connections.teradata import TeradataHandler

import pandas as pd
from collections import OrderedDict
import openpyxl
from openpyxl.styles import Font, PatternFill
from datetime import datetime


class Waterfall:
    def __init__(
            self,
            conditions,
            offer_code,
            campaign_planner,
            lead,
            waterfall_location,
            sql_constructor: SQLConstructor,
            teradata_connection: TeradataHandler
    ):
        self._sqlconstructor = SQLConstructor
        self._teradata_connection = teradata_connection

        self.offer_code = offer_code
        self.campaign_planner = campaign_planner
        self.lead = lead
        self._sql_constructor = sql_constructor
        self.waterfall_location = waterfall_location

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
        # Create a new workbook and select the active worksheet
        wb = openpyxl.Workbook()
        ws = wb.active

        # Fill the cells according to the requirements
        ws['A1'] = f'[{self.offer_code}] CP: {self.campaign_planner} LEAD: {self.lead}'
        ws['A1'].font = Font(size=16)
        ws['A2'] = 'Check #'
        ws['A3'] = 'Check'
        ws['A4'] = 'Check Description'
        ws['C3'] = 'Starting Population'

        # Fill the values from conditions_df
        for i, (index, row) in enumerate(self._conditions.iterrows(), start=4):
            ws[f'B{i}'] = row['column_name']
            ws[f'C{i}'] = row['description']

        # Format the headers in row 2
        light_blue_fill = PatternFill(start_color='ADD8E6', end_color='ADD8E6', fill_type='solid')
        for cell in ws[2]:
            cell.fill = light_blue_fill

        # Insert a blank column with gray background
        col_index = 4
        gray_fill = PatternFill(start_color='D3D3D3', end_color='D3D3D3', fill_type='solid')
        ws.cell(row=4, column=col_index).fill = gray_fill

        # Insert data from compiled_dataframes with the required formatting
        row_index = 4
        channel_groups = {}
        for identifier in self._compiled_dataframes.keys():
            channel = identifier.split('_')[0]
            if channel not in channel_groups:
                channel_groups[channel] = []
            channel_groups[channel].append(identifier)

        check_number = 1
        for channel, identifiers in channel_groups.items():
            for identifier in identifiers:
                df = self._compiled_dataframes[identifier]
                col_index += 1  # Move to the next column after the blank column
                for col_num, col_name in enumerate(df.columns, start=col_index):
                    ws.cell(row=2, column=col_num).value = col_name  # Fill the column headers in row 2
                    ws.cell(row=2, column=col_num).fill = light_blue_fill  # Set header background to light blue
                    for df_row_num, value in enumerate(df[col_name], start=4):
                        ws.cell(row=df_row_num, column=col_num).value = value
                        ws.cell(row=df_row_num, column=1).value = check_number  # Number the rows
                        check_number += 1

                col_index += len(df.columns)  # Move to the next blank column after the dataframe

            row_index += len(df) + 1  # Move the row index for the next dataframe

            # Insert a blank row for new channel
            ws.cell(row=row_index, column=1).value = channel
            ws.cell(row=row_index, column=1).fill = light_blue_fill
            ws.cell(row=row_index, column=2).fill = light_blue_fill
            ws.cell(row=row_index, column=3).fill = light_blue_fill
            row_index += 1

        # Save the workbook to the specified filename
        wb.save(f'{self.waterfall_location}/{self.offer_code}_Waterfall_{datetime.now().strftime("%Y%m%d")}.xlsx')
