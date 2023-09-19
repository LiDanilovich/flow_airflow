import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs
from datetime import timedelta, datetime

def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql import functions as F, Window
    from pyspark.sql.types import StringType

    df = dfc.select(list(dfc.keys())[0]).toDF()

    df = df.withColumn("dt_ms", F.unix_timestamp(F.col("dt").cast("timestamp")) * 1000)
    
    sdf = DynamicFrame.fromDF(df, glueContext, "")

    return DynamicFrameCollection({"CustomTransform0": sdf}, glueContext)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# compruebo si recibo max_partition en la invocación, caso contrario asigno valor por default

if ('--{}'.format('max_partition') in sys.argv):
    maxp = getResolvedOptions(sys.argv, ['max_partition'])
else:
    maxp = {'max_partition': (datetime.today()-timedelta(days=1)).strftime('%Y-%m-%d')}
    
max_partition = maxp['max_partition']

print("filto a aplicar: ", max_partition) 

DataCatalogtable_node_assets = glueContext.create_dynamic_frame.from_catalog(
    database="db-stage-prod",
    table_name="insights_assets_lcpr",
    additional_options = {
        "catalogPartitionPredicate":f"dt = '{max_partition}'"
    }
)

DropDuplicates_node1686089400506 = DynamicFrame.fromDF(
    DataCatalogtable_node_assets.toDF().dropDuplicates(),
    glueContext
)

ApplyMapping_node_assets = ApplyMapping.apply(
    frame=DropDuplicates_node1686089400506,
    mappings=[
        ("pkg_cde_pks", "string", "pkg_cde_pks", "string"),
        ("play", "string", "play", "string"),
        ("pkg_descr", "string", "pkg_descr", "string"),
        ("serv_seq_no_pks", "int", "serv_seq_no_pks", "int"),
        ("serv_cde_pks", "string", "serv_cde_pks", "string"),
        ("serv_cde_online_descr", "string", "serv_cde_online_descr", "string"),
        ("chrg_pks", "double", "chrg_pks", "double"),
        ("service_code_length", "int", "service_code_length", "int"),
        ("pkg_code_length", "int", "pkg_code_length", "int"),
        ("service_code_lob", "string", "service_code_lob", "string"),
        ("dt", "string", "dt", "string"),
    ]
)

CustomTransform_node_assets = MyTransform(
    glueContext,
    DynamicFrameCollection({"ApplyMapping_node_assets": ApplyMapping_node_assets}, glueContext),
)

SelectFromCollection_node_assets = SelectFromCollection.apply(
    dfc=CustomTransform_node_assets,
    key=list(CustomTransform_node_assets.keys())[0]
)

pre_query = "begin;"
pre_query += f"delete from prod.public.insights_assets_lcpr where dt='{max_partition}';"
pre_query += "end;"

my_conn_options = {
    "dbtable": "insights_assets_lcpr",
    "database": "prod",
    "aws_iam_role": "arn:aws:iam::283229902738:role/service-role/AmazonRedshift-CommandsAccessRole-20230522T161159",
    "preactions": pre_query
}

glueContext.write_dynamic_frame.from_jdbc_conf(
    frame = SelectFromCollection_node_assets, 
    catalog_connection = "cdp-lcpr", 
    connection_options = my_conn_options,
    redshift_tmp_dir = args["TempDir"]
)

job.commit()