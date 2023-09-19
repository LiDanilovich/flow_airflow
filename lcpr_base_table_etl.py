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

max_partition = (datetime.today()-timedelta(days=1)).strftime('%Y-%m-%d')

dyf = glueContext.create_dynamic_frame.from_catalog(
    database="db-stage-prod",
    table_name="insights_customer_services_rates_lcpr",
    additional_options = {
        "useS3ListImplementation":False,
        "catalogPartitionPredicate":f"dt = '{max_partition}'"
    }
)

DropDuplicates_node = DynamicFrame.fromDF(
    dyf.toDF().dropDuplicates(),
    glueContext
)

ApplyMapping_node = ApplyMapping.apply(
    frame=DropDuplicates_node,
    mappings=[
        ("sub_acct_no_sbb", "long", "sub_acct_no_sbb", "long")
    ]
)

CustomTransform_node = MyTransform(
    glueContext,
    DynamicFrameCollection(
        {"ApplyMapping_node":ApplyMapping_node}, glueContext
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