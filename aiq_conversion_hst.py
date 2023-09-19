import sys
from datetime import date, timedelta, datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame


args = getResolvedOptions(sys.argv, ["JOB_NAME", "bucket", "prefix", "key"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

bucket = args['bucket']
prefix = args['prefix']
key = args['key']

from pyspark.sql import functions as F


s3_uri = f"s3://{bucket}/{prefix}{key}"

df_aiq = spark.read.option("header","true").option("delimiter", ",").csv([s3_uri])

today = (datetime.today() - timedelta(days=0))

year = str(today.year)
month = str(today.month)
day = str(today.day)
hour_min_sec = str(today.hour) + str(today.minute) + str(today.second)

df_aiq.createOrReplaceTempView("df_aiq")
df_aiq_final = spark.sql(f""" select *, 
             '{year + month }' as month,
             '{year + month + day}' as day,
             '{hour_min_sec}' as hour
             from df_aiq""")

if df_aiq_final.count() > 0:
       df_aiq_final = df_aiq_final.repartition("month", "day", "hour")

       df_aiq_final \
         .write.mode('append') \
         .format('parquet') \
         .partitionBy('month', 'day', 'hour') \
         .save( "s3://cdp-lcpr-process/hist/aiq_conversion/" )
else:
    print("empty conversion audience")

query = spark.sql("""MSCK REPAIR TABLE db_dev_cdp_project.aiq_conversion""")

job.commit()
