import random
import string
from typing import Dict, List, Tuple, Set, Any, Optional
from collections import OrderedDict

from pythonwf.construct_sql.eligibilty import EligibilitySQLConstructor
from pythonwf.construct_sql.waterfall import WaterfallSQLConstructor
from pythonwf.construct_sql.output_file import OutputFileSQLConstructor
from pythonwf.validations.construct_sql import ConstructSQLMeta
from pythonwf.logging.logging import call_logger, CustomLogger


class SQLConstructor(metaclass=ConstructSQLMeta):
    """
    A class to construct various SQL queries required for eligibility and waterfall processes.

    Attributes:
        conditions (OrderedDict): Conditions for eligibility checks.
        tables (Dict[str, List[Dict[str, Any]]]): Tables involved in the eligibility and waterfall processes.
        unique_identifiers (List[str]): Unique identifiers used in the processes.
        _username (str): The username.
        _backend_tables (Optional[Dict[str, str]]): Backend table details.
        _EligibilitySQLConstructor (Optional[EligibilitySQLConstructor]): SQL constructor for eligibility.
        _WaterfallSQLConstructor (Optional[WaterfallSQLConstructor]): SQL constructor for waterfall.
        _OutputFileSQLConstructor (Optional[OutputFileSQLConstructor]): SQL constructor for output files.
        _conditions_column_mappings (Dict[str, Any]): Mappings of conditions to columns.
        _parsed_unique_identifiers (Dict[str, Any]): Parsed unique identifiers.
    """

    def __init__(
            self,
            conditions: OrderedDict,
            tables: Dict[str, List[Dict[str, Any]]],
            unique_identifiers: List[str],
            username: str,
            logger: CustomLogger
    ):
        """
        Initializes the SQLConstructor class with the provided parameters.

        Args:
            conditions (OrderedDict): Conditions for eligibility checks.
            tables (Dict[str, List[Dict[str, Any]]]): Tables involved in the eligibility and waterfall processes.
            unique_identifiers (List[str]): Unique identifiers used in the processes.
            username (str): The username.
        """
        # properties used just for validations
        self._conditions_column_mappings = dict()
        self._parsed_unique_identifiers = dict()

        # set properties
        self.logger = logger
        self._username = username
        self.conditions = conditions
        self.tables = tables
        self.unique_identifiers = unique_identifiers
        self._backend_tables = {}
        self._generate_backend_table_details()

        # prep properties for constructors
        self._EligibilitySQLConstructor: EligibilitySQLConstructor or None = None
        self._WaterfallSQLConstructor: WaterfallSQLConstructor or None = None
        self._OutputFileSQLConstructor = None

    def _generate_table_name(self) -> str:
        """
        Generates a random table name for backend and user work tables.

        Returns:
            str: Table name formatted as "user_work.<username>_<random 10-character alphanumeric string>".
        """
        random_string = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
        table_name = f"user_work.{self._username}_{random_string}"
        return table_name

    @call_logger()
    def _generate_backend_table_details(self) -> None:
        """
        Generates backend table details and stores them in the _backend_tables attribute.
        """
        need_names = ['eligibility']
        need_names.extend(self._parsed_unique_identifiers.get('original_without_aliases'))

        self._backend_tables = {}
        for table in need_names:
            self._backend_tables[table] = self._generate_table_name()

    # @call_logger()
    def _assimilate_tables(self, tables: Dict[str, List[Dict[str, Any]]]) -> Tuple[
        List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Combines the tables and user work table details into a single dictionary.

        Args:
            tables (Dict[str, List[Dict[str, Any]]]): The tables to assimilate.

        Returns:
            Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]: Combined tables and work tables.
        """
        work_tables = tables.get('work_tables')
        for table in work_tables:
            join_type = table.get('join_type')
            alias = table.get('alias')
            where_conditions = table.get('where_conditions')
            join_conditions = table.get('join_conditions')
            table_name = self._generate_table_name()
            table['table_name'] = table_name

            table_entry = {
                'table_name': table_name,
                'join_type': join_type,
                'alias': alias,
                'where_conditions': where_conditions,
                'join_conditions': join_conditions
            }

            if join_type == 'FROM':
                tables.get('tables').insert(0, table_entry)
            else:
                tables.get('tables').append(table_entry)

        return tables.get('tables'), tables.get('work_tables')

    @call_logger('parsed_unique_identifiers')
    def _parse_unique_identifiers(self) -> None:
        """
        Parses the unique identifiers and stores the parsed values in the _parsed_unique_identifiers attribute.
        """
        without_aliases: Set[str] = set()
        with_aliases: Set[str] = set()
        original_without_aliases: List[str] = []

        for identifier in self._unique_identifiers:
            parts = [part.strip() for part in identifier.split(',')]
            original_parts_without_aliases = [part.split('.')[1] for part in parts]
            original_without_aliases.append(', '.join(original_parts_without_aliases))
            for part in parts:
                alias, column = part.split('.')
                without_aliases.add(column)
                with_aliases.add(part)

        parsed_unique_identifiers = {
            "without_aliases": without_aliases,
            "with_aliases": with_aliases,
            "original_without_aliases": set(original_without_aliases)
        }

        self._parsed_unique_identifiers = parsed_unique_identifiers

    @call_logger('final_result')
    def _prepare_conditions(self) -> None:
        """
        Adds the column_name to each condition and creates a reference dictionary with these names.
        """
        column_naming_convention = "{channel}_{template}_{num}"
        column_names: Set[str] = set()
        for channel, templates in self.conditions.items():
            for template, checks in templates.items():
                for num, check in enumerate(checks, start=1):
                    column_name = column_naming_convention.format(channel=channel, template=template, num=num)
                    check['column_name'] = column_name
                    column_names.add(column_name)

        # create dictionary of each check with all corresponding relevant checks
        result: Dict[str, Any] = {}

        for column_name in column_names:
            selected_checks_list: List[str] = []

            def add_checks(channels: List[str]) -> None:
                for channel in channels:
                    for template, checks in self.conditions[channel].items():
                        for check in checks:
                            if check['column_name'] not in selected_checks_list:
                                selected_checks_list.append(check['column_name'])

            selected_channel: Optional[str] = None
            selected_template: Optional[str] = None
            templates_order: List[str] = []

            for channel, templates in self.conditions.items():
                templates_order.append(channel)
                for template, checks in templates.items():
                    if column_name in [check['column_name'] for check in checks]:
                        selected_channel = channel
                        selected_template = template
                        break
                if selected_channel:
                    break

            if not selected_channel:
                result[column_name] = "Check not found"
                continue

            # Ensure 'BA' template is included in the base waterfall
            if selected_channel == 'main' or selected_template == 'BA':
                add_checks(['main', selected_channel])
            else:
                add_checks(['main'])
                for channel, templates in self.conditions.items():
                    if channel != 'main':
                        for template, checks in templates.items():
                            if template == selected_template or template == 'BA':
                                for check in checks:
                                    if check['column_name'] not in selected_checks_list:
                                        selected_checks_list.append(check['column_name'])

            if column_name in selected_checks_list:
                selected_checks_list.remove(column_name)

            prior_templates: Dict[str, Any] = {}
            post_templates: Dict[str, Any] = {}

            # Build prior templates excluding 'BA'
            for template in self.conditions[selected_channel]:
                if template == selected_template:
                    break
                if template != 'BA':
                    no_output = [check['column_name'] for check in self.conditions[selected_channel][template] if
                                 not check['output']]
                    output = [check['column_name'] for check in self.conditions[selected_channel][template] if
                              check['output']]
                    prior_templates[template] = {'no_output': no_output, 'output': output}

            # Build post templates excluding 'BA'
            for template in list(self.conditions[selected_channel].keys())[
                            list(self.conditions[selected_channel].keys()).index(selected_template) + 1:]:
                if template != 'BA':
                    no_output = [check['column_name'] for check in self.conditions[selected_channel][template] if
                                 not check['output']]
                    output = [check['column_name'] for check in self.conditions[selected_channel][template] if
                              check['output']]
                    post_templates[template] = {'no_output': no_output, 'output': output}

            result[column_name] = {
                'base': selected_checks_list,
                'prior_templates': prior_templates,
                'post_templates': post_templates
            }

        conditions_column_mappings = result

        self._conditions_column_mappings = conditions_column_mappings

    @property
    def conditions(self) -> OrderedDict:
        """Getter for conditions."""
        return self._conditions

    @conditions.setter
    def conditions(self, value: OrderedDict) -> None:
        """Setter for conditions."""
        self._conditions = value
        self._prepare_conditions()

    @property
    def backend_tables(self) -> dict[str, str]:
        return self._backend_tables

    @backend_tables.setter
    def backend_tables(self, value: dict[str, str]) -> None:
        self._backend_tables = value

    @property
    def tables(self) -> List[Dict[str, Any]]:
        """Getter for tables."""
        return self._tables

    @tables.setter
    def tables(self, tables: Dict[str, List[Dict[str, Any]]]) -> None:
        """Setter for tables."""
        tables, work_tables = self._assimilate_tables(tables)
        self._tables: List = tables
        self.work_tables: List = work_tables

    @property
    def work_tables(self) -> List[Dict[str, Any]]:
        """Getter for work_tables."""
        return self._work_tables

    @work_tables.setter
    def work_tables(self, values: List[Dict[str, Any]]) -> None:
        """Setter for work_tables."""
        self._work_tables = values

    @property
    def unique_identifiers(self) -> List[str]:
        """Getter for unique_identifiers."""
        return self._unique_identifiers

    @unique_identifiers.setter
    def unique_identifiers(self, value: List[str]) -> None:
        """Setter for unique_identifiers."""
        self._unique_identifiers = value
        self._parse_unique_identifiers()

    @property
    @call_logger()
    def waterfall(self) -> WaterfallSQLConstructor:
        """Getter for waterfall."""
        if self._WaterfallSQLConstructor is None:
            self._WaterfallSQLConstructor = WaterfallSQLConstructor(
                self.conditions,
                self._conditions_column_mappings,
                self._backend_tables,
                self._parsed_unique_identifiers,
                self.logger
            )
        return self._WaterfallSQLConstructor

    @property
    def output_file(self) -> OutputFileSQLConstructor:
        """Getter for output_file."""
        if self._OutputFileSQLConstructor is None:
            self._OutputFileSQLConstructor = OutputFileSQLConstructor()
        return self._OutputFileSQLConstructor

    @property
    def eligible(self) -> EligibilitySQLConstructor:
        """Getter for eligible."""
        if self._EligibilitySQLConstructor is None:
            self._EligibilitySQLConstructor = EligibilitySQLConstructor(
                self.conditions,
                self.tables,
                self.work_tables,
                self._backend_tables.get('eligibility'),
                self._parsed_unique_identifiers,
                self.logger
            )

        return self._EligibilitySQLConstructor
