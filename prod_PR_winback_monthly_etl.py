import sys
import pandas as pd
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue.dynamicframe import DynamicFrameCollection
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME", "file"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql import functions as F, Window
    from pyspark.sql.types import StringType

    df = dfc.select(list(dfc.keys())[0]).toDF()
    df = df.withColumn("account_id", F.concat(F.lit("LCPR_FX_"), F.col("sub_acct_no_sbb")))
    
    df = df.withColumn("disconnection_date_ms", F.unix_timestamp(F.col("disconnection_date").cast("timestamp")) * 1000)

    sdf = DynamicFrame.fromDF(df, glueContext, "")
    return DynamicFrameCollection({"CustomTransform0": sdf}, glueContext)

s3_uri = args["file"]
key = s3_uri.split("/")[-1] 

df = spark.read.option("header","true").option("delimiter", ",").csv([s3_uri])

pandas_df = df.toPandas()
pandas_df["upload_date"] = key[19:-4]
pandas_df.to_csv("s3://cdp-lcpr-process/hist/winback_hst/" + key, sep='\t', index=False)

dyf = DynamicFrame.fromDF(df, glueContext, "")

ApplyMapping_node_w = ApplyMapping.apply(
    frame=dyf,
    mappings=[
        ("sub_acct_no_sbb", "string", "sub_acct_no_sbb", "string"),
        ("nombre", "string", "nombre", "string"),
        ("email", "string", "email", "string"),
        ("home_phone_sbb", "string", "home_phone_sbb", "string"),
        ("disconnection date", "string", "disconnection_date", "string"),
        ("bef_value_amt", "string", "bef_value_amt", "double"),
        ("disco_rsn_sbb", "string", "disco_rsn_sbb", "string"),
        ("hasvideo", "string", "hasvideo", "boolean"),
        ("hashsd", "string", "hashsd", "boolean"),
        ("hasvoice", "string", "hasvoice", "boolean"),
        ("internet_down", "string", "internet_down", "string"),
        ("cable_down", "string", "cable_down", "string"),
        ("phone_down", "string", "phone_down", "string"),
    ]
)

CustomTransform_node_w = MyTransform(
    glueContext,
    DynamicFrameCollection(
        {"ApplyMapping_node_w":ApplyMapping_node_w}, glueContext
    ),
)

SelectFromCollection_node_w = SelectFromCollection.apply(
    dfc=CustomTransform_node_w,
    key=list(CustomTransform_node_w.keys())[0]
)

pre_query = "delete from prod.public.winback"

my_conn_options = {
    "dbtable": "winback",
    "database": "prod",
    "aws_iam_role": "arn:aws:iam::283229902738:role/service-role/AmazonRedshift-CommandsAccessRole-20230522T161159",
    "preactions" : pre_query
}

glueContext.write_dynamic_frame.from_jdbc_conf(
    frame = SelectFromCollection_node_w, 
    catalog_connection = "cdp-lcpr",
    connection_options = my_conn_options,
    redshift_tmp_dir = args["TempDir"]
)

job.commit()
