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
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

taxi_catalogs = '["yellow", "green"]'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, to_utc_timestamp, to_date, exists, lit, hour, day, replace, when, round, year, quarter,month, make_date, date_format, weekofyear, dayofyear, dayofmonth, dayofweek
from pyspark.sql import functions as F, Window, Row
from pyspark.sql.types import DoubleType, LongType, IntegerType
from pyspark.errors import AnalysisException
from datetime import datetime
from dateutil.relativedelta import relativedelta
import json

bronze_lh = 'abfss://Itransition@onelake.dfs.fabric.microsoft.com/Bronze_Lakehouse.Lakehouse/'
taxi_catalogs = json.loads(taxi_catalogs)
begin_date = '2020-01-01'
end_date = datetime.today().date() + relativedelta(years=1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def columnOrDefault(df, col_name: str, default_value, cast_type):
    return df.withColumn(col_name, round(F.abs(col(col_name)), 2) if col_name in df.columns else lit(default_value).cast(cast_type))

@F.udf(returnType=DoubleType())
def fahr_to_celsius(fahr):
    return (fahr - 32) * 5.0 / 9.0

def filter_taxi(taxi):
    return taxi.filter(taxi.fare < 1000000)

def clean_taxi(taxi, color: str, year_month: str):
    y, m = year_month.split('-')
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
        make_date(lit(y), lit(m), day(pu)).alias('pu_dt'),
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

def fill_missing_dates(df):
    all_dates_df = df.groupBy("id").agg(
        F.max(F.to_date("date", "yyyy-MM-dd")).alias("max_date"),
        F.min(F.to_date("date", "yyyy-MM-dd")).alias("min_date")
    ).select("id", F.expr("sequence(min_date, max_date, interval 1 day)").alias("date")
    ).withColumn("date", F.explode("date")
    ).withColumn("date", F.date_format("date", "yyyy-MM-dd"))
    return all_dates_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

default_first = '1900-01.parquet'
try:
    meta_df = spark.sql("SELECT * FROM Silver_Lakehouse.dbo.Meta")
except AnalysisException:
    meta_df = spark.createDataFrame([
        Row(table_name='DimTrip', field_name='yellow', last='1900-01.parquet'),
    ])
for c in taxi_catalogs:
    try:
        last = meta_df.filter(meta_df.field_name == c).first().last
    except AttributeError:
        last = default_first
        new_record = spark.createDataFrame([
            Row(table_name='DimTrip', field_name=c, last=default_first),
        ])
        meta_df = meta_df.union(new_record)
    for f in notebookutils.fs.ls(bronze_lh + f'Files/{c}/'):
        if f.name > last:
            trip = spark.read.parquet(bronze_lh + f'Files/{c}/{f.name}')
            trip = clean_taxi(trip, c, f.name.split('.')[0])
            trip = filter_taxi(trip)
            trip.write.format('delta').mode('append').save('Tables/dbo/DimTrip')
            
            meta_df = meta_df.withColumn('last',
                when((meta_df['field_name'] == c) & (meta_df['table_name'] == 'DimTrip') , f.name).otherwise(meta_df['last']))
            meta_df.write.format('delta').mode('overwrite').save('Tables/dbo/Meta')


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
                           when(dayofweek(col('date')).isin(1,7), 'True').otherwise('False').alias('is_weekend'))

time_dim.write.format('delta').mode('overwrite').save('Tables/dbo/DimDate')

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
        col('value'),
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

gdp = (
    spark.sql("SELECT * FROM Bronze_Lakehouse.dbo.GDP")
    .select(col('date').alias('year'), col('value').alias('gdp'))
    )
gdp.write.format('delta').mode('overwrite').save('Tables/dbo/DimGDP')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fx = (
    spark.sql("SELECT * FROM Bronze_Lakehouse.dbo.ECB")
    .select(col('date'), col('ecb').alias('fx'))
    )
fx = fx.withColumn('id', lit(0))
all_dates_df = fill_missing_dates(fx)

w = Window.partitionBy("id").orderBy("date")

fx = all_dates_df.join(fx, ["id", "date"], "left").select(
    "date",
    *[F.last(F.col(c), ignorenulls=True).over(w).alias(c)
      for c in fx.columns if c not in ("id", "date")
     ]
)

fx.write.format('delta').mode('overwrite').save('Tables/dbo/DimFX')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

temperature = spark.sql("SELECT * FROM Bronze_Lakehouse.dbo.Temperature OFFSET 2")
temperature = (
    temperature.select(
        to_date(temperature[0], 'yyyyMM').alias('date'), 
        temperature[1].alias('avg_fahr'),
    )
)

precipitation = spark.sql("SELECT * FROM Bronze_Lakehouse.dbo.Precipitation OFFSET 2")
precipitation = (
    precipitation.select(
        to_date(precipitation[0], 'yyyyMM').alias('date'), 
        precipitation[1].alias('sum_precipitation')
    )
)

weather = (
    temperature
        .withColumn('avg_celsius', round(fahr_to_celsius(col('avg_fahr')), 1))
        .join(precipitation, 'date')
)
weather.write.format('delta').mode('overwrite').save('Tables/dbo/DimWeather')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
