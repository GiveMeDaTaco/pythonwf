# Usage Documentation for the Library

## Overview

This library is designed to handle eligibility checks and waterfall analysis for campaign management. It includes several classes and functions to facilitate SQL generation, data handling, and logging.

## Table of Contents

1. [Classes and Functions](#classes-and-functions)
    - [Eligible](#eligible)
    - [Waterfall](#waterfall)
    - [Output](#output)
    - [SQLConstructor](#sqlconstructor)
    - [TeradataHandler](#teradatahandler)
    - [CustomLogger](#customlogger)
2. [Usage Examples](#usage-examples)

## Classes and Functions

### Eligible

The `Eligible` class handles eligibility operations in a campaign, using SQL generation and Teradata connections.

#### Attributes:
- `campaign_planner (str)`: The campaign planner.
- `lead (str)`: The lead person.
- `username (str)`: The username.
- `offer_code (str)`: The offer code.
- `conditions (OrderedDict)`: The conditions for eligibility.
- `tables (dict)`: The tables involved in the eligibility check.
- `unique_identifiers (list)`: Unique identifiers used in the eligibility check.
- `_teradata_connection (TeradataHandler)`: The Teradata connection handler.
- `_sqlconstructor (SQLConstructor)`: An instance of SQLConstructor to build SQL queries.

#### Methods:
- `generate_eligibility()`: Generates eligibility by creating work tables and executing the eligibility SQL.

### Waterfall

The `Waterfall` class generates and analyzes waterfall reports for campaign eligibility checks.

#### Attributes:
- `conditions (pd.DataFrame)`: Conditions for eligibility checks.
- `offer_code (str)`: The offer code.
- `campaign_planner (str)`: The campaign planner.
- `lead (str)`: The lead person.
- `waterfall_location (str)`: The location to save the waterfall report.
- `_sqlconstructor (SQLConstructor)`: An instance of SQLConstructor to build SQL queries.
- `_teradata_connection (Optional[TeradataHandler])`: The Teradata connection handler.
- `_column_names (OrderedDict)`: Column names for the waterfall report.
- `_query_results (Dict[str, List[pd.DataFrame]])`: Query results for each identifier.
- `_compiled_dataframes (OrderedDict)`: Compiled dataframes for each identifier.
- `_starting_population (Optional[int])`: The starting population for the waterfall analysis.
- `_combined_df (Optional[pd.DataFrame])`: Combined dataframe for the waterfall report.

#### Methods:
- `generate_waterfall()`: Generates the waterfall report by creating base tables, analyzing eligibility, creating dataframes, and saving the report to an Excel file.

### Output

The `Output` class handles the creation of output files based on the eligibility and waterfall analysis.

#### Attributes:
- `conditions (dict)`: Conditions for eligibility checks.
- `sqlconstructor`: An instance of SQLConstructor to build SQL queries.
- `logger (CustomLogger)`: The logger instance.
- `teradata_connection (TeradataHandler)`: The Teradata connection handler.
- `_output_instructions (dict)`: Instructions for generating output files.
- `_output_queries (dict)`: SQL queries for generating output files.

#### Methods:
- `create_output_file()`: Outputs the file for the specified channel(s).

### SQLConstructor

The `SQLConstructor` class constructs various SQL queries required for eligibility and waterfall processes.

#### Attributes:
- `conditions (OrderedDict)`: Conditions for eligibility checks.
- `tables (Dict[str, List[Dict[str, Any]]])`: Tables involved in the eligibility and waterfall processes.
- `unique_identifiers (List[str])`: Unique identifiers used in the processes.
- `_username (str)`: The username.
- `_backend_tables (Optional[Dict[str, str]])`: Backend table details.
- `_EligibilitySQLConstructor (Optional[EligibilitySQLConstructor])`: SQL constructor for eligibility.
- `_WaterfallSQLConstructor (Optional[WaterfallSQLConstructor])`: SQL constructor for waterfall.
- `_OutputFileSQLConstructor (Optional[OutputFileSQLConstructor])`: SQL constructor for output files.
- `_waterfall_conditions_column_mappings (Dict[str, Any])`: Mappings of conditions to columns.
- `_parsed_unique_identifiers (Dict[str, Any])`: Parsed unique identifiers.

#### Methods:
- `generate_backend_table_details()`: Generates backend table details and stores them in the `_backend_tables` attribute.

### TeradataHandler

The `TeradataHandler` class handles connections and operations with Teradata.

#### Attributes:
- `host (str)`: The Teradata host.
- `user (str)`: The username.
- `password (str)`: The password.
- `logmech (str)`: The log mechanism.
- `context`: The Teradata context.
- `connection`: The Teradata connection.
- `tracking (TrackSQL)`: The SQL tracking instance.
- `teradataml_version (str)`: The version of the Teradata ML library.

#### Methods:
- `connect()`: Establishes a connection to Teradata.
- `disconnect()`: Disconnects from Teradata.
- `execute_query(query)`: Executes a SQL query.
- `to_pandas(query)`: Converts a SQL query result to a Pandas DataFrame.
- `fastexport(query)`: Exports data using Teradata's FastExport utility.
- `cleanup()`: Cleans up tracked tables and disconnects from Teradata.

### CustomLogger

The `CustomLogger` class provides custom logging functionality.

#### Attributes:
- `logger`: The logger instance.

#### Methods:
- `info(message)`: Logs an info message.
- `error(message)`: Logs an error message.

## Usage Examples

### Example 1: Generating Eligibility

```python
import json
from pythonwf.eligibility.eligibility import Eligible
from pythonwf.logging.logging import CustomLogger

with open('sample_conditions.json', 'r') as f:
    conditions = json.load(f)

with open('sample_tables.json', 'r') as f:
    tables = json.load(f)

offer_code = 'TST0001'
campaign_planner = 'Michael McConkie'
lead = "Richard Bartels"
username = 'nbk1234'
unique_identifiers = ['a.column1', 'b.column2', 'a.column1, b.column2']
logger = CustomLogger(__name__)

eligible = Eligible(campaign_planner, lead, username, offer_code, conditions, tables, unique_identifiers, logger)
eligible.generate_eligibility()
```

### Example 2: Generating Waterfall

```python
from pythonwf.waterfall.waterfall import Waterfall

waterfall = Waterfall.from_eligible(eligible, '.')
waterfall.generate_waterfall()
```

### Example 3: Creating Output File

```python
from pythonwf.output.output import Output

output = Output.from_waterfall(waterfall)

output_arguments = {
    'channel1': {
        'sql': 'SELECT * FROM eligibility_table',
        'file_location': '.',
        'file_base_name': 'SOME_OFFER_NAME',
        'output_options': {'format': 'csv', 'additional_arguments': ''}
    }
}
output.output_instructions = output_arguments
output.create_output_file()
```

This documentation provides an overview of the library's classes and functions, along with usage examples to help you get started.
