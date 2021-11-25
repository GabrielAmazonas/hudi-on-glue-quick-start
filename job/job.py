import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, asc, desc
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext


import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# The KryoSerializer is necessary for the SparkSession to be able to serialize objects


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
        .getOrCreate()
    return spark


# Spark + Glue context configuration
spark = create_spark_session()
sc = spark.sparkContext
glueContext = GlueContext(sc)

# Cloud Formation Template Parameters
args = getResolvedOptions(sys.argv,
                          ['base_s3_path'])

# Simulation of Delta Lake Layers
base_s3_path = args['base_s3_path']
target_s3_path = "{base_s3_path}/tmp/hudi_trips_target".format(
    base_s3_path=base_s3_path)

table_name = "hudi_trips_cow"
final_base_path = "{base_s3_path}/tmp/hudi_trips_cow".format(
    base_s3_path=base_s3_path)

# DataSet Generation
dataGen = spark._jvm.org.apache.hudi.QuickstartUtils.DataGenerator()

inserts = spark._jvm.org.apache.hudi.QuickstartUtils.convertToStringList(
    dataGen.generateInserts(10))
df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))

hudi_options = {
    'hoodie.table.name': table_name,
    'hoodie.datasource.write.recordkey.field': 'uuid',
    'hoodie.datasource.write.partitionpath.field': 'partitionpath',
    'hoodie.datasource.write.table.name': table_name,
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.datasource.write.precombine.field': 'ts',
    'hoodie.upsert.shuffle.parallelism': 2,
    'hoodie.insert.shuffle.parallelism': 2,
}

# Writing HUDI table
df.write.format("hudi").options(
    **hudi_options).mode("overwrite").save(final_base_path)

# Reading HUDI table data
# pyspark
tripsSnapshotDF = spark. \
    read. \
    format("hudi"). \
    load(final_base_path)
# load(final_base_path) use "/partitionKey=partitionValue" folder structure for Spark auto partition discovery

print("Snapshot Count: " + str(tripsSnapshotDF.count()))

tripsSnapshotDF.createOrReplaceTempView("hudi_trips_snapshot")

# Query examples using Spark SQL
spark.sql("select fare, begin_lon, begin_lat, ts from  hudi_trips_snapshot where fare > 20.0").show()
spark.sql("select _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path, rider, driver, fare from  hudi_trips_snapshot").show()

### Time Travel Query Examples ###
time_travel_df = spark.read.format("hudi").option(
    "as.of.instant", "20210728141108").load(final_base_path)

time_travel_df = spark.read.format("hudi").option(
    "as.of.instant", "2021-07-28 14:11:08").load(final_base_path)

# It is equal to "as.of.instant = 2021-07-28 00:00:00"
time_travel_df = spark.read.format("hudi").option(
    "as.of.instant", "2021-07-28").load(final_base_path)

hudi_trips_df = spark.sql(
    "select _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path, rider, driver, fare from  hudi_trips_snapshot")

### Update Data ###

updates = sc._jvm.org.apache.hudi.QuickstartUtils.convertToStringList(
    dataGen.generateUpdates(10))

df = spark.read.json(spark.sparkContext.parallelize(updates, 2))

df.write.format("hudi"). \
    options(**hudi_options). \
    mode("append"). \
    save(final_base_path)

### Incremental Query ###

spark. \
  read. \
  format("hudi"). \
  load(final_base_path). \
  createOrReplaceTempView("hudi_trips_snapshot")

commits = list(map(lambda row: row[0], spark.sql("select distinct(_hoodie_commit_time) as commitTime from  hudi_trips_snapshot order by commitTime").limit(50).collect()))
beginTime = commits[len(commits) - 2] # commit time we are interested in

# incrementally query data
incremental_read_options = {
  'hoodie.datasource.query.type': 'incremental',
  'hoodie.datasource.read.begin.instanttime': beginTime,
}

tripsIncrementalDF = spark.read.format("hudi"). \
  options(**incremental_read_options). \
  load(final_base_path)

tripsIncrementalDF.createOrReplaceTempView("hudi_trips_incremental")

# Demonstration of Incremental Query Results
print("Incremental Query Results:")
spark.sql("select `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts from  hudi_trips_incremental where fare > 20.0").show()

#This will give all changes that happened after the beginTime commit with the filter of fare > 20.0.

### Point in time query ###

# pyspark
beginTime = "000" # Represents all commits > this time.
endTime = commits[len(commits) - 2]

# query point in time data
point_in_time_read_options = {
  'hoodie.datasource.query.type': 'incremental',
  'hoodie.datasource.read.end.instanttime': endTime,
  'hoodie.datasource.read.begin.instanttime': beginTime
}

tripsPointInTimeDF = spark.read.format("hudi"). \
  options(**point_in_time_read_options). \
  load(final_base_path)

tripsPointInTimeDF.createOrReplaceTempView("hudi_trips_point_in_time")

print("Point in time query results:")
spark.sql("select `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts from hudi_trips_point_in_time where fare > 20.0").show()

print("Time to Glue Catalog with Spark SQL")

### Spark SQL + Glue Data Catalog ###
spark.sql(f"CREATE DATABASE IF NOT EXISTS hudi_demo")

spark.sql(f"DROP TABLE IF EXISTS hudi_demo.hudi_trips")

print(f"Table hudi_demo.hudi_trips dropped, re-creating it")

spark.sql(
    f"CREATE TABLE IF NOT EXISTS hudi_demo.hudi_trips USING PARQUET LOCATION '{target_s3_path}' as (SELECT * from hudi_trips_snapshot)")

print(f"Table hudi_demo.hudi_trips created")
