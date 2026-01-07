import fabric.functions as fn
import logging
import json

udf = fn.UserDataFunctions()

@udf.function()
def check_variables(var: str) -> str:
    logging.info('Python UDF trigger function processed a request.')

    return f"Welcome to Fabric Functions: {var}"


@udf.function()
def get_first_sorted(childs: str) -> str:
    jchilds = json.loads(childs.replace('\\', ''))
    return sorted(c['name'] for c in jchilds)[0]

@udf.function()
def get_last_sorted(childs: str) -> str:
    jchilds = json.loads(childs.replace('\\', ''))
    return sorted(c['name'] for c in jchilds)[-1]