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
# META           "id": "7fca3f40-0732-49d4-93d1-55cbe9734873"
# META         },
# META         {
# META           "id": "606efa16-c9eb-4ff7-bbbc-861c5d767260"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import col, to_utc_timestamp, to_date, exists, lit, hour, replace, when, round, year, quarter,month, date_format, weekofyear, dayofyear, dayofmonth, dayofweek
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, IntegerType

bronze_lh = 'abfss://Itransition@onelake.dfs.fabric.microsoft.com/Bronze_Lakehouse.Lakehouse/'
catalogs = ['yellow', 'green']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df = spark.sql("SELECT * FROM Bronze_Lakehouse.dbo.Nsensors")
# df.write.format('delta').mode('overwrite').save(bronze_lh + 'Tables/dbo/Sensors')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
