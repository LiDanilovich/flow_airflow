import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue.dynamicframe import DynamicFrameCollection
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


DataCatalogtable_node_2023 = glueContext.create_dynamic_frame.from_catalog(
    database="db-stage-dev",
    table_name="2023_lcpr_frontbook"
)

DropDuplicates_node_2023 = DynamicFrame.fromDF(
    DataCatalogtable_node_2023.toDF().dropDuplicates(),
    glueContext
)

ApplyMapping_node_2023 = ApplyMapping.apply(
    frame=DropDuplicates_node_2023,
    mappings=[
        ("csg_codes", "string", "csg_codes", "string"),
        ("online_desc", "string", "online_desc", "string"),
        ("statement_desc", "string", "statement_desc", "string"),
        ("connection_type", "string", "connection_type", "string"),
        ("play", "string", "play", "string"),
        ("tv", "string", "tv", "string"),
        ("phone", "string", "phone", "string"),
        ("modem", "string", "modem", "string"),
        ("download_speed", "string", "download_speed", "string"),
        ("upload_speed", "string", "upload_speed", "string"),
        ("tv_box", "long", "tv_box", "long"),
        ("regular", "double", "regular", "double"),
        ("is_double_speed", "string", "is_double_speed", "string"),
        ("comments", "string", "comments", "string"),
        ("stbs_for_migration-->", "long", "stbs_for_migration-->", "long"),
        ("hd", "string", "hd", "string"),
        ("hd_rate", "long", "hd_rate", "long"),
        ("dvr", "string", "dvr", "string"),
        ("dvr_rate", "long", "dvr_rate", "long"),
        ("hubtv", "string", "hubtv", "string"),
        ("hubtv_rate", "long", "hubtv_rate", "long"),
    ],
)

my_conn_options = {
    "dbtable": "2023_lcpr_frontbook",
    "database": "prod",
    "aws_iam_role": "arn:aws:iam::283229902738:role/service-role/AmazonRedshift-CommandsAccessRole-20230522T161159"
}

glueContext.write_dynamic_frame.from_jdbc_conf(
    frame = ApplyMapping_node_2023, 
    catalog_connection = "cdp-lcpr",
    connection_options = my_conn_options,
    redshift_tmp_dir = args["TempDir"]
)

job.commit()
