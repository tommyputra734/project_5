from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("project5Tommy").getOrCreate()

df = spark.read.load('/home/dev/airflow/spark-code/Project-5/fhv_tripdata_2021-02.parquet')

#1 How many taxi trips were there on February 15?
df1 = df.withColumn("date",to_date("pickup_datetime"))
print('Total Trips on February 15 are ' + str(df1.where(df1.date == '2021-02-15').count()))

#2 Find the longest trip for each day ?
df2 = df1.withColumn('pickUp_timestamp', to_timestamp(col('pickup_datetime')))\
    .withColumn('dropOff_timestamp', to_timestamp(col('dropOff_datetime')))\
    .withColumn('time_trip_sec',col("dropOff_timestamp").cast("long")-col('pickUp_timestamp').cast("long"))
    
df2.createOrReplaceTempView("retail")
result = spark.sql("SELECT date, max(time_trip_sec) FROM retail group by date order by date")
result.show()

#3 Find Top 5 Most frequent `dispatching_base_num` ?
df.groupby('dispatching_base_num').count().orderBy('count', ascending=False).show(5)

#4 Find Top 5 Most common location pairs (PUlocationID and DOlocationID) ?
dfFilter = df.filter(df.PUlocationID.isNotNull() & df.DOlocationID.isNotNull())
dfFilter.groupby('PUlocationID', 'DOlocationID').count().orderBy('count', ascending=False).show(5)