import json
from pythonwf.eligibility.eligibility import Eligible
from pythonwf.logging.logging import CustomLogger
from pythonwf.waterfall.waterfall import Waterfall
from pythonwf.output.output import Output

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
waterfall = Waterfall.from_eligible(eligible, '.')
waterfall.generate_waterfall()
output = Output.from_waterfall(waterfall)

output_arguments = {
    'channel1': {
        'sql': 'SELECT * FROM eligibility_table',
        'file_location': '.',
        'file_base_name': 'SOME_OFFER_NAME',
        'output_options': {'format': 'csv', 'additional_arguments': ''}
    }
}
print(type(output))
output.output_instructions = output_arguments
output.create_output_file()



