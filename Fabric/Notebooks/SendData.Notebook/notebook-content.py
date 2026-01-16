# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "8baef6f1-6896-a10b-4a24-92a7f7f0c2d9",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

user = ''
password = ''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

options = {
  'url': 'jdbc:mysql://92.5.58.169:7006/Fabric',
  'driver':'com.mysql.jdbc.Driver',
  'user': user,
  'password': password
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

weather = spark.read.synapsesql('Gold_Warehouse.Gold.DimWeather')
weather.write.format('jdbc').options(**options, dbtable='Weather').mode('overwrite').save()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.functions import col, round

trips = spark.read.synapsesql('Gold_Warehouse.Gold.FactTaxiDaily')
trips = (
    trips
    .groupBy(col('weather_id'))
    # .groupBy(col('date'))
    .agg(
        F.make_date(F.year(F.first('date')), F.month(F.first('date')), F.lit(1)).alias('date'),
        round(F.sum('sum_fares'), 2).alias('sum_fares'),
        F.sum('count_trips').alias('count_trips'),
        # F.first('weather_id').alias('weather_id'),
    )
    .where(col('date').isNotNull())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

out = (
    trips
    .join(weather, trips.weather_id == weather.weather_id, 'inner')
    .orderBy(col('date'))
    .select('date', 'sum_fares', 'count_trips', 'avg_fahr', 'avg_celsius', 'sum_precipitation')
)
out.write.format('jdbc').options(**options, dbtable='Data').mode('overwrite').save()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
