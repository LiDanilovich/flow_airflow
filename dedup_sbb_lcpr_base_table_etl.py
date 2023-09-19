import os, sys, time, boto3, json, subprocess
from datetime import datetime, date, timedelta
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame


def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql import functions as F, Window
    from pyspark.sql.types import StringType

    df = dfc.select(list(dfc.keys())[0]).toDF()
    df = df.withColumn("account_id", F.concat(F.lit("LCPR_FX_"), F.col("sub_acct_no_sbb"))).drop("sub_acct_no_sbb")
    df = df.withColumn("org_cntry", F.lit("LCPR"))
    df = df.withColumn("mkt", F.lit("FIXED"))

    sdf = DynamicFrame.fromDF(df, glueContext, "")
    return DynamicFrameCollection({"CustomTransform0": sdf}, glueContext)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


dyf = glueContext.create_dynamic_frame.from_catalog(
    database="db-stage-prod",
    table_name="insights_customer_services_rates_lcpr",
    additional_options = {
        "useS3ListImplementation":False,
        "catalogPartitionPredicate":f"dt >= '2023-07-04'"
    }
)


ApplyMapping_node = ApplyMapping.apply(
    frame=dyf,
    mappings=[
        ("sub_acct_no_sbb", "long", "sub_acct_no_sbb", "long")
    ]
)

today = datetime.today()
date = str(today.year) + str(today.month).zfill(2)
winback_file = f"s3://cdp-lcpr-flags-bucket/winback/PR_winback_monthly_{date}01.csv"

try:
    df_winback = spark.read.option("header","true").option("delimiter", ",").csv([winback_file]).select("SUB_ACCT_NO_SBB")
    df_winback = df_winback.withColumnRenamed("SUB_ACCT_NO_SBB","sub_acct_no_sbb")
    df_winback.createOrReplaceTempView("df")

    df_base = df_winback.union(ApplyMapping_node.toDF())
    dyf_base = DynamicFrame.fromDF(df_base, glueContext, "")

except:
    print("ARCHIVO DE WINBACK NO ENCONTRADO")
    dyf_base = ApplyMapping_node

DropDuplicates_node = DynamicFrame.fromDF(
    dyf_base.toDF().dropDuplicates(),
    glueContext
)

CustomTransform_node = MyTransform(
    glueContext,
    DynamicFrameCollection(
        {"DropDuplicates_node":DropDuplicates_node}, glueContext
    ),
)

pandas_df = SelectFromCollection.apply(
    dfc=CustomTransform_node,
    key=list(CustomTransform_node.keys())[0]
).toDF().toPandas()


fechaCSV = datetime.now().strftime("%Y_%m_%d")
outCSV = "s3://aiq-exchange-lcpr/base_table/lcpr_fixed_base_" + fechaCSV + ".csv"
pandas_df.to_csv(outCSV, encoding='utf-8', index=False)

job.commit()