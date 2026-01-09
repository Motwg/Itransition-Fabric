# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "606efa16-c9eb-4ff7-bbbc-861c5d767260",
# META       "default_lakehouse_name": "Silver_Lakehouse",
# META       "default_lakehouse_workspace_id": "2c02544d-0315-499e-9895-af59e611b26b",
# META       "known_lakehouses": [
# META         {
# META           "id": "606efa16-c9eb-4ff7-bbbc-861c5d767260"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "07a74f6b-f276-b04e-491a-92665e2d7362",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

webhook_id = ''
webhook_url = ''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import great_expectations as gx


def definitions(df_expectations: dict[str, list[gx.ExpectationSuite]]) -> dict[str, gx.ValidationDefinition]:
    validation_definitions = {}
    for df_name, expectations in df_expectations.items():
        data_source = context.data_sources.add_spark(df_name + '_spark_datasource')
        data_asset = data_source.add_dataframe_asset(df_name + '_asset')
        batch_params = {'dataframe': spark.read.table(df_name)}
        batch_definition = data_asset.add_batch_definition_whole_dataframe(df_name + '_batch_definition')
        batch_definition.build_batch_request(batch_params)

        expectation_suite_name = df_name + '_validation_suite'
        context.suites.add(suite := gx.ExpectationSuite(expectation_suite_name))

        validation_definitions[df_name] = (val_def := gx.ValidationDefinition(
            data = batch_definition,
            suite = suite,
            name = df_name + '_validation_definition',
        ))
        context.validation_definitions.add(val_def)
        for expectation in expectations:
            suite.add_expectation(expectation)
    return validation_definitions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

context = gx.get_context()

expectations = {
    'DimDate': [
        gx.expectations.ExpectColumnValuesToBeUnique(column='dt'),
        gx.expectations.ExpectColumnValuesToBeBetween(column='quarter', min_value=1, max_value=4),
    ],
    'DimSensors': [
        gx.expectations.ExpectColumnValuesToNotBeNull(column='sensor_id'),
        gx.expectations.ExpectColumnValuesToNotBeNull(column='zone_id'),
        gx.expectations.ExpectColumnValuesToNotBeNull(column='value'),
        gx.expectations.ExpectColumnValuesToBeBetween(column='value', min_value=0.0, max_value=1000.0),
    ],
    'DimTrip': [
        gx.expectations.ExpectColumnValuesToBeBetween(column='fare', min_value=0.0, max_value=1_000_000.0),
        gx.expectations.ExpectColumnValuesToNotBeNull(column='pu_dt'),
        gx.expectations.ExpectColumnValuesToNotBeNull(column='pu_hour'),
        gx.expectations.ExpectColumnValuesToBeBetween(column='pu_hour', min_value=0, max_value=23),
    ],
}

val_definitions = definitions(expectations)

results = {}
for table, val_definition in val_definitions.items():
    checkpoint = gx.Checkpoint(
        name=table + '_checkpoint',
        validation_definitions=[val_definition],
        actions=[],
    )
    results[table] = checkpoint.run({'dataframe': spark.read.table(table)})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pprint

def generate_details(run_result):
    lines = []
    for _, desc in run_result.items():
        for i, exp_result in enumerate(desc['results']):
            exp = exp_result.expectation_config
            lines.append(f'\n[{i + 1}] {exp.type}: ')
            lines.append(f'Status: {"success" if exp_result.success else "fail"}')
            lines.append(f'\tParameters: {pprint.pformat(exp.kwargs)}')
            lines.append(f'\tResult: {pprint.pformat(exp_result.result, compact=True, indent=8)}')

    return '\n'.join(lines)


reports = [
    f'''    === Table: {table} === 
Success: {"SUCCESS" if result.success else "FAIL"}
{generate_details(result.run_results)}'''
for table, result in results.items()
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests

url = f'https://discord.com/api/webhooks/{webhook_id}/{webhook_url}'
limit_chars = 1990
for report in reports:
    for i in range(0, len(report), limit_chars):
        requests.post(url, data={'content': '.\n' + report[i:i + limit_chars]})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
