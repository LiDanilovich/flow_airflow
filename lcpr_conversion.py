import sys
import os
from datetime import date, timedelta, datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

from pyspark.sql import functions as F


if ('--bucket' in sys.argv and '--prefix' in sys.argv and '--key' in sys.argv):
    args = getResolvedOptions(sys.argv, ['bucket', 'prefix', 'key'])

    bucket = args['bucket']
    prefix = args['prefix']
    key = args['key']

    s3_uri = f"s3://{bucket}/{prefix}{key}"
    df_aiq = spark.read.option("header","true").option("delimiter", ",").csv([s3_uri])

elif('--month' in sys.argv and '--day' in sys.argv and '--hour' in sys.argv):
    args = getResolvedOptions(sys.argv, ['month', 'day', 'hour'])
    month = args['month']
    day = args['day']
    hour = args['hour']
    df_aiq = spark.sql(f"""select * from db_dev_cdp_project.aiq_conversion where month={month} and day={day} and hour={hour}""")

else:
    print("Parametros invalidos")
    job.commit()
    os._exit(1)


df_c = glueContext.create_dynamic_frame.from_catalog(
    database="db_dev_cdp_project",
    table_name="conv_communications_hist"
).toDF()

df_c.createOrReplaceTempView("df_c")

df_converted = spark.sql("""
select * from df_c
where conv_flg = true""")

df_not_converted = spark.sql("""
select * from df_c
where conv_flg is not true""")

df_c_1 = df_not_converted.drop("conv_use_case_cat","order_id", "conversion_dt", "conversion_type")

df_aiq = df_aiq.select("account_id", "conv_use_case_cat", "order_id", "conv_dte", "conv_type")

df_j = df_c_1.join(df_aiq, on="account_id", how="left")

df_j = df_j.withColumnRenamed("conv_type", "conversion_type")

for col_name in df_j.columns:
    df_j = df_j.withColumnRenamed(col_name, col_name.lower())

df_j = df_j.withColumn("conversion_dt", F.from_unixtime(F.col("conv_dte") / 1000))
df_j = df_j.withColumn("contact_dt", F.to_timestamp(F.col("contact_dt")))
df_j = df_j.drop("conv_dte")
df_j = df_j.unionByName(df_converted)

df_j.createOrReplaceTempView("df_j")

channel_hierarchy = [
    ("email", 1),
    ("Outbound call", 2),
    ("WhatsApp", 3),
    ("Inbound call", 4)
]

df_channel_weights = spark.createDataFrame(channel_hierarchy, ["channel", "channel_weight"])
df_j = df_j.join(df_channel_weights, on="channel", how="left")

df_min_weigth = df_j.groupBy("account_id").agg(F.min("channel_weight").alias("min_channel_weight"))

df_j_w_min = df_j.join(df_min_weigth, on="account_id", how="left")

df_j_w_min = df_j_w_min.withColumn(
    "conv_flg",
    F.when(
        (F.col("conv_flg") == True),
        F.col("conv_flg")
    ).otherwise(
        (
            (F.col("campaign_name").contains(F.col("conv_use_case_cat"))) &
            (F.datediff(F.col("conversion_dt"), F.col("contact_dt")) <= 14) &
            (F.datediff(F.col("conversion_dt"), F.col("contact_dt")) >= 0) &
            (F.col("channel_weight") == F.col("min_channel_weight"))
        )
    )
)
df_j_w_min.createOrReplaceTempView("df_j_w_min")
df_j_w_min = df_j_w_min.drop("channel_weight", "min_channel_weight")

if df_j_w_min.count() > 0:
    df_j_w_min = df_j_w_min.repartition("month", "day", "channel")

    df_j_w_min \
        .write.mode('overwrite') \
        .format('parquet') \
        .partitionBy('month', 'day', 'channel') \
        .save( "s3://cdp-lcpr-process/hist/bkp_conv_communications_hist/" )

    df = spark.read.parquet('s3://cdp-lcpr-process/hist/bkp_conv_communications_hist/')
    df = df.repartition("month", "day", "channel")

    df \
        .write.mode('overwrite') \
        .format('parquet') \
        .partitionBy('month', 'day', 'channel') \
        .save( "s3://cdp-lcpr-process/hist/conv_communications_hist/" )
else:
    print("no records at the output")

query = spark.sql("""MSCK REPAIR TABLE db_dev_cdp_project.conv_communications_hist""")

job.commit()
