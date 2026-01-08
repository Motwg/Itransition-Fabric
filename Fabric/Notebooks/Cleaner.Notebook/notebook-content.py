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

def columnOrDefault(df, col_name: str, default_value, cast_type):
    return df.withColumn(col_name, round(F.abs(col(col_name)), 2) if col_name in df.columns else lit(default_value).cast(cast_type))

def clean_taxi(taxi, color: str):
    opt_columns = [
        'total_amount',
        'fare_amount',
        'extra',
        'mta_tax',
        'tip_amount',
        'tolls_amount',
        'improvement_surcharge',
        'congestion_surcharge',
        'Airport_fee',
        'cbd_congestion_fee',
    ]
    for c in opt_columns:
        taxi = columnOrDefault(taxi, c, 0.0, DoubleType())
    prefix = {
        'yellow': 'tpep',
        'green': 'lpep'
    }.get(color)
    pu = to_utc_timestamp(col(prefix + '_pickup_datetime'), 'America/New_York')
    cleaned = taxi.select(
        col('VendorID').cast('integer').alias('vendor_id'),
        to_date(pu).alias('pu_dt'),
        hour(pu).cast('integer').alias('pu_hour'),
        col('passenger_count').cast('integer').alias('passengers'),
        col('trip_distance').alias('distance'),
        col('RatecodeID').cast('integer').alias('ratecode_id'),
        col('store_and_fwd_flag').alias('store_flag'),
        col('PULocationID').cast('integer').alias('pu_loc_id'),
        col('DOLocationID').cast('integer').alias('do_loc_id'),
        col('payment_type').cast('integer').alias('payment_type'),
        col('total_amount').alias('total'),
        col('fare_amount').alias('fare'),
        col('tip_amount').alias('tips'),
        col('extra').alias('extra'),
        col('mta_tax').alias('tax'),
        col('tolls_amount').alias('tolls'),
        col('improvement_surcharge').alias('improvement_surcharge'),
        col('congestion_surcharge').alias('congestion_surcharge'),
        col('airport_fee').alias('airport_fee'),
        col('cbd_congestion_fee').alias('congestion_fee'),
    ).dropDuplicates()
    return cleaned

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

zone_dict = {'EWR':'Unknown','N/A':'Unknown'}
zones = (
    spark.read.format("csv").option("header","true").load(bronze_lh + "Files/zone_lookup.csv")
    .replace(zone_dict, 1)
    .select(
        col('LocationID').cast('long').alias('zone_id'),
        col('Borough').alias('borough'),
        col('Zone').alias('zone'),
        col('service_zone')
    )
)
zones.write.format('delta').mode('overwrite').save('Tables/dbo/DimZone')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

begin_date = '2020-01-01'
end_date = '2029-12-31'

time_dim = (
    spark.sql(f"select explode(sequence(to_date('{begin_date}'), to_date('{end_date}'), interval 1 day)) as date")
)
time_dim = time_dim.select(col('date').alias('dt'), 
                           year(col('date')).alias('year'), 
                           quarter(col('date')).alias('quarter'), 
                           month(col('date')).alias('month'), 
                           date_format(col('date'), 'MMMM').alias('month_name'), 
                           weekofyear(col('date')).alias('week_of_year'),
                           dayofyear(col('date')).alias('day_of_year'), 
                           dayofmonth(col('date')).alias('day'), 
                           dayofweek(col('date')).alias('day_of_week'), 
                           date_format(col('date'),'EEEE').alias('day_name'), 
                           when(dayofweek(col('date')).isin(1,7), 'Yes').otherwise('No').alias('is_weekend'))

time_dim.write.format('delta').mode('overwrite').save('Tables/dbo/DimDate')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# x = spark.read.parquet(bronze_lh + "Files/green")
# display(x)
for c in catalogs:
    for f in notebookutils.fs.ls(bronze_lh + f'Files/{c}/'):
        trip = spark.read.parquet(bronze_lh + f'Files/{c}/{f.name}')
        trip = clean_taxi(trip, c)
        trip.write.format('delta').mode('append').save('Tables/dbo/DimTrip')
# fees_cols = (c for c in ('congestion_surcharge', 'extra', 'tolls_amount', 'airport_fee', 'cbd_congestion_fee', 'improvement_surcharge', 'mta_tax') if c in yellow.columns)
# yellow = yellow.withColumn('fees', F.expr('+'.join(fees_cols)))




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sensors = (
    spark.sql("SELECT * FROM Bronze_Lakehouse.dbo.Sensors")
    .select(
        col('sensor_id'),
        col('zone').alias('zone_id'),
        to_date(col('from_utc')).alias('dt'),
        hour(col('from_utc')).cast('integer').alias('pu_hour'),
        col('name'),
        col('units'),
    )
)
sensors.write.format('delta').mode('overwrite').save('Tables/dbo/DimSensors')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fx = (
    spark.sql("SELECT * FROM Bronze_Lakehouse.dbo.ECB")
    .select(col('date').alias('dt'), col('ecb').alias('fx'))
    )
gdp = (
    spark.sql("SELECT * FROM Bronze_Lakehouse.dbo.GDP")
    .select(col('date').alias('year_dt'), col('value').alias('gdp'))
    )
fx.write.format('delta').mode('overwrite').save('Tables/dbo/DimFX')
gdp.write.format('delta').mode('overwrite').save('Tables/dbo/DimGDP')

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
