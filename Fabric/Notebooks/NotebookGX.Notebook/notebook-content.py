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

# Welcome to your new notebook
# Type here in the cell editor to add code!
value = 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

value = meta_df.filter(meta_df.field_name == c).first().last
mssparkutils.notebook.exit()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import great_expectations as gx


def definitions(df_expectations: dict[str, list[gx.ExpectationSuite]]):
    validation_definitions = []
    for df_name, expectations in df_expectations.items():
        data_source = context.data_sources.add_spark(df_name + '_spark_datasource')
        data_asset = data_source.add_dataframe_asset(df_name + '_asset')
        batch_params = {'dataframe': spark.read.table(df_name)}
        batch_definition = data_asset.add_batch_definition_whole_dataframe(df_name + '_batch_definition')
        batch_definition.build_batch_request(batch_params)

        expectation_suite_name = df_name + '_validation_suite'
        context.suites.add(suite := gx.ExpectationSuite(expectation_suite_name))

        validation_definitions.append(val_def := gx.ValidationDefinition(
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
        gx.expectations.ExpectColumnValuesToBeBetween(column='value', min_value=-1.0, max_value=1000.0),
    ]
}

val_definitions = definitions(expectations)
action_list = [
]

checkpoint = gx.Checkpoint(
    name="my_checkpoint",
    validation_definitions=val_definitions,
    actions=action_list,
    # result_format={"result_format": "BASIC", "unexpected_index_column_names": ["hash_col"]},
)

validation_results = checkpoint.run({'dataframe': spark.read.table('DimSensors')})
validation_results
# results = validation_definition.run(batch_parameters = {'dataframe': df})
# results

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
