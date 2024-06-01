import random
import string

from pythonwf.construct_sql.eligibilty import EligibilitySQLConstructor
from pythonwf.construct_sql.waterfall import WaterfallSQLConstructor
from pythonwf.construct_sql.output_file import OutputFileSQLConstructor
from pythonwf.validations.construct_sql import ConstructSQLMeta


class SQLConstructor(metaclass=ConstructSQLMeta):
    def __init__(self, conditions, tables, unique_identifiers, username):
        self._username = username
        self.conditions = conditions
        self.tables = tables
        self.unique_identifiers = unique_identifiers
        self._backend_tables = None

        # prep properties for constructors
        self._EligibilitySQLConstructor = None
        self._WaterfallSQLConstructor = None
        self._OutputFileSQLConstructor = None

        # properties used just for validations
        self._conditions_column_mappings = dict()
        self._parsed_unique_identifiers = dict()


    def _generate_table_name(self):
        """
        Used to create random table names for backend and user work tables

        :return str: table name formatted "user_work.<NBK>_<random 10-character alphanumeric string>
        """
        random_string = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
        table_name = f"user_work.{self._username}_{random_string}"
        return table_name

    def _generate_backend_table_details(self):
        need_names = ['eligibility']
        need_names.append(self._parsed_unique_identifiers.get('original_without_alias'))

        for table in need_names:
            self._backend_tables[table] = self._generate_table_name()

    def _assimilate_tables(self, tables):
        """
        Combines the tables and user work table details into a single dictionary

        :return:
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

    def _parse_unique_identifiers(self):
        without_aliases = set()
        with_aliases = set()
        original_without_alias = []

        for identifier in self._unique_identifiers:
            parts = [part.strip() for part in identifier.split(',')]
            original_parts_without_aliases = [part.split('.')[1] for part in parts]
            original_without_alias.append(', '.join(original_parts_without_aliases))
            for part in parts:
                alias, column = part.split('.')
                without_aliases.add(column)
                with_aliases.add(part)

        self._parsed_unique_identifiers = {
            "without_aliases": without_aliases,
            "with_aliases": with_aliases,
            "original_without_alias": original_without_alias
        }

    def _prepare_conditions(self):
        """
        Adds the column_name to each condition (channel_template_number);
        Creates a reference dictionary with these names

        :return:
        """
        column_naming_convention = "{channel}_{template}_{num}"
        column_names = []
        for channel, templates in self.conditions.items():
            for template, checks in templates.items():
                for num, check in enumerate(checks, start=1):
                    column_name = column_naming_convention.format(channel=channel, template=template, num=num)
                    check['column_name'] = column_name
                    checks.append(column_name)

        # create dictionary of each check with all corresponding relevant checks
        result = {}

        for column_name in column_names:
            selected_checks_list = []

            def add_checks(channels):
                for channel in channels:
                    for template, checks in self.conditions[channel].items():
                        for check in checks:
                            if check['column_name'] not in selected_checks_list:
                                selected_checks_list.append(check['column_name'])

            selected_channel = None
            selected_template = None
            templates_order = []

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
                add_checks(['main', 'channel'])
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

            prior_templates = {}
            post_templates = {}

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

        self._conditions_column_mappings = result

    @property
    def conditions(self):
        return self._conditions

    @conditions.setter
    def conditions(self, value):
        self._conditions = value
        self._prepare_conditions()

    @property
    def tables(self):
        return self._tables

    @tables.setter
    def tables(self, tables):
        tables, work_tables = self._assimilate_tables(tables)
        self._tables = tables
        self.work_tables = work_tables

    @property
    def work_tables(self):
        return self._tables

    @work_tables.setter
    def work_tables(self, tables):
        self._work_tables = tables

    @property
    def unique_identifiers(self):
        return self._unique_identifiers

    @unique_identifiers.setter
    def unique_identifiers(self, value):
        self._unique_identifiers = value
        self._parse_unique_identifiers()

    @property
    def waterfall(self):
        if self._WaterfallSQLConstructor is None:
            self._WaterfallSQLConstructor = WaterfallSQLConstructor(
                self.conditions,
                self._conditions_column_mappings,
                self._backend_tables,
                self._parsed_unique_identifiers
            )
        return self._WaterfallSQLConstructor

    @property
    def output_file(self):
        if self._OutputFileSQLConstructor is None:
            self._OutputFileSQLConstructor = OutputFileSQLConstructor()
        return self._OutputFileSQLConstructor

    @property
    def eligible(self):
        if self._EligibilitySQLConstructor is None:
            self._EligibilitySQLConstructor = EligibilitySQLConstructor(
                self.conditions,
                self.tables,
                self._backend_tables.get('eligibility'),
                self._parsed_unique_identifiers
            )
        return self._EligibilitySQLConstructor


