import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, asc, desc
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext
from faker import Faker


import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Initialize Faker for performance evaluation

# Cloud Formation Template Parameters
args = getResolvedOptions(sys.argv,
                          ['base_s3_path', 'fake_row_count'])

# Simulation of Delta Lake Layers
base_s3_path = args['base_s3_path']

table_name = "hudi_employee"

target_s3_path = "{base_s3_path}/tmp/hudi_employee_target".format(
    base_s3_path=base_s3_path)
final_base_path = "{base_s3_path}/tmp/hudi_employee".format(
    base_s3_path=base_s3_path)

fake_row_count = int(args['fake_row_count'])

fake = Faker()

fake_workers = [(
        x,
        fake.name(),
        fake.random_element(elements=('IT', 'HR', 'Sales', 'Marketing')),
        fake.random_element(elements=('CA', 'NY', 'TX', 'FL', 'IL', 'RJ')),
        fake.random_int(min=10000, max=150000),
        fake.random_int(min=18, max=60),
        fake.random_int(min=0, max=100000),
        fake.unix_time()
      ) for x in range(fake_row_count)]

print("Fake workers sample: \n") 
print(fake_workers)

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


columns= ["emp_id", "employee_name","department","state","salary","age","bonus","ts"]
emp_df = spark.createDataFrame(data = fake_workers, schema = columns)

hudi_options = {
    'hoodie.table.name': table_name,
    'hoodie.datasource.write.recordkey.field': 'emp_id',
    'hoodie.datasource.write.partitionpath.field': 'state',
    'hoodie.datasource.write.table.name': table_name,
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.datasource.write.precombine.field': 'ts',
    'hoodie.upsert.shuffle.parallelism': 2,
    'hoodie.insert.shuffle.parallelism': 2,
}


# Writing HUDI table
emp_df.write.format("hudi").options(
    **hudi_options).mode("overwrite").save(final_base_path)

# Reading HUDI table data
# pyspark
empSnapshotDF = spark. \
    read. \
    format("hudi"). \
    load(final_base_path)
# load(final_base_path) use "/partitionKey=partitionValue" folder structure for Spark auto partition discovery

print("Snapshot Count: " + str(empSnapshotDF.count()))

empSnapshotDF.createOrReplaceTempView("hudi_employee_snapshot")

# Query examples using Spark SQL
spark.sql("select _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path, emp_id, employee_name from  hudi_employee_snapshot").show()

# Upsert records into hudi table
simpleDataUpd = [
    (3, "Gabriel","Sales","RJ",81000,30,23000,827307999), \
    (7, "Paulo","Engineering","RJ",79000,53,15000,1627694678), \
  ]

columns= ["emp_id", "employee_name","department","state","salary","age","bonus", "ts"]
emp_up_df = spark.createDataFrame(data = simpleDataUpd, schema = columns)

emp_up_df.write.format("hudi"). \
    options(**hudi_options). \
    mode("append"). \
    save(final_base_path)

print("Employee Update Results:")
emp_df_read = spark.read.format("hudi").load(final_base_path)
print(emp_df_read.show())

emp_df_read.createOrReplaceTempView("hudi_employee_view")

print("Time to Glue Catalog with Spark SQL")


### Spark SQL + Glue Data Catalog ###
spark.sql(f"CREATE DATABASE IF NOT EXISTS hudi_demo")

spark.sql(f"DROP TABLE IF EXISTS hudi_demo.hudi_employee")

spark.sql(
    f"CREATE TABLE IF NOT EXISTS hudi_demo.hudi_employee USING PARQUET LOCATION '{target_s3_path}' as (SELECT * from hudi_employee_view)")

print(f"Table hudi_demo.hudi_employee created")

print("Hudi Employee Data:")
print(spark.sql(f"SELECT * FROM hudi_demo.hudi_employee").show(10))

