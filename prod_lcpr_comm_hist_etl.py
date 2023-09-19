import sys
from datetime import datetime, date, timedelta
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql import functions as F, Window
    from pyspark.sql.types import StringType

    df = dfc.select(list(dfc.keys())[0]).toDF()
   
    df = df.withColumn("dt_ms", F.unix_timestamp(F.to_timestamp("dt", "yyyymmdd")) * 1000)
    df = df.withColumn("sent_dt_ms", F.unix_timestamp("sent_dt") * 1000).drop("sent_dt")
    df = df.withColumn("contact_dt_ms", F.unix_timestamp("contact_dt") * 1000).drop("contact_dt")
    df = df.withColumn("conv_dt_ms", F.unix_timestamp("conv_dt") * 1000).drop("conv_dt")
    df = df.withColumn("rec_dt_ms", F.unix_timestamp(F.to_timestamp("rec_dt", "d-M-yyyy")) * 1000).drop("rec_dt")
 

    sdf = DynamicFrame.fromDF(df, glueContext, "")
    return DynamicFrameCollection({"CustomTransform0": sdf}, glueContext)


DataCatalogtable_node_toa = glueContext.create_dynamic_frame.from_catalog(
    database="db_dev_cdp_project",
    table_name="communications_hist"
)

DataCatalogtable_node_toa.toDF().createOrReplaceTempView("df")

if ('--{}'.format('max_partition') in sys.argv):
    maxp = getResolvedOptions(sys.argv, ['max_partition'])
    
elif ('--{}'.format('days_before') in sys.argv):
    args = getResolvedOptions(sys.argv, ['days_before'])
    days_before = int(args["days_before"])
    maxp = {'max_partition': (datetime.today()-timedelta(days=days_before)).strftime('%Y%m%d')}
else:
    #q_max_date = """select max(ls_chg_dte_ocr) from df;"""
    #max_partition = spark.sql(q_max_date).collect()[0][0]
    maxp = {'max_partition': (datetime.today()-timedelta(days=0)).strftime('%Y%m%d')}


max_partition = maxp['max_partition']

print('Insertando datos del: ', max_partition)

q_max_partition = f"""select * from df where day = '{max_partition}';"""
res = spark.sql(q_max_partition)

dyf = DynamicFrame.fromDF(res, glueContext, "dynamic_frame_name")
dyf.printSchema()

DropDuplicates_node_toa = DynamicFrame.fromDF(
    dyf.toDF().dropDuplicates(),
    glueContext
)

ApplyMapping_node_toa = ApplyMapping.apply(
    frame=DropDuplicates_node_toa,
    mappings=[
        ("account_id", "string", "account_id", "string"),
        ("channel", "string", "channel", "string"),
        ("campaign_name", "string", "campaign", "string"),
        ("use_case", "string", "use_case", "string"),
        ("regime", "string", "regime", "string"),
        ("sent_dt", "timestamp", "sent_dt", "timestamp"),
        ("contact_dt", "timestamp", "contact_dt", "timestamp"),
        ("conversion_dt", "timestamp", "conv_dt", "timestamp"),
        ("conversion_type", "string", "conv_type", "string"),
        ("rec_date", "date", "rec_dt", "date"),
        ("order_id", "string", "order_id", "string"),
        ("conv_use_case_cat", "string", "conv_use_case_category", "string"),
        ("day", "string", "dt", "string")
        ]
)

CustomTransform_node_toa = MyTransform(
    glueContext,
    DynamicFrameCollection(
        {"ApplyMapping_node_toa":ApplyMapping_node_toa}, glueContext
    ),
)

SelectFromCollection_node_toa = SelectFromCollection.apply(
    dfc=CustomTransform_node_toa,
    key=list(CustomTransform_node_toa.keys())[0]
)

pre_query = "begin;"
pre_query += f"delete from prod.public.lcpr_communications_hst where dt={max_partition};"
pre_query += "end;"

my_conn_options = {
    "dbtable": "lcpr_communications_hst",
    "database": "prod",
    "aws_iam_role": "arn:aws:iam::283229902738:role/service-role/AmazonRedshift-CommandsAccessRole-20230522T161159",
    "preactions": pre_query
}

glueContext.write_dynamic_frame.from_jdbc_conf(
    frame = SelectFromCollection_node_toa, 
    catalog_connection = "cdp-lcpr", 
    connection_options = my_conn_options, 
    redshift_tmp_dir = args["TempDir"]
)

job.commit()
