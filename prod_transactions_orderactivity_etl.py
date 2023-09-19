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
    df = df.withColumn("account_id", F.concat(F.lit("LCPR_FX_"), F.col("sub_acct_no_ooi"))).drop("sub_acct_no_ooi")
    
    df = df.withColumn("ls_chg_dte_ocr_ms", F.unix_timestamp(F.col("ls_chg_dte_ocr").cast("timestamp")) * 1000)
    df = df.withColumn("create_dte_ocr_ms", F.unix_timestamp(F.col("create_dte_ocr").cast("timestamp")) * 1000)
    df = df.withColumn("disco_dte_sbb_ms", F.unix_timestamp(F.col("disco_dte_sbb").cast("timestamp")) * 1000)
    

    sdf = DynamicFrame.fromDF(df, glueContext, "")
    return DynamicFrameCollection({"CustomTransform0": sdf}, glueContext)


DataCatalogtable_node_toa = glueContext.create_dynamic_frame.from_catalog(
    database="db-stage-dev",
    table_name="transactions_orderactivity"
)

DataCatalogtable_node_toa.toDF().createOrReplaceTempView("df")

if ('--{}'.format('max_partition') in sys.argv):
    maxp = getResolvedOptions(sys.argv, ['max_partition'])
else:
    #q_max_date = """select max(ls_chg_dte_ocr) from df;"""
    #max_partition = spark.sql(q_max_date).collect()[0][0]

    maxp = {'max_partition': (datetime.today()-timedelta(days=1)).strftime('%Y-%m-%d')}
    
max_partition = maxp['max_partition']

q_max_partition = f"""select * from df where ls_chg_dte_ocr = '{max_partition}';"""
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
        ("ls_chg_dte_ocr", "string", "ls_chg_dte_ocr", "string"),
        ("create_dte_ocr", "string", "create_dte_ocr", "string"),
        ("ord_stat_ocr", "string", "ord_stat_ocr", "string"),
        ("order_no_ooi", "long", "order_no_ooi", "long"),
        ("acct_type", "string", "acct_type", "string"),
        ("sub_acct_no_ooi", "long", "sub_acct_no_ooi", "long"),
        ("disco_rsn_sbb", "string", "disco_rsn_sbb", "string"),
        ("disco_dte_sbb", "string", "disco_dte_sbb", "string"),
        ("order_create_operator", "string", "order_create_operator", "string"),
        ("order_salesrep", "long", "order_salesrep", "long"),
        ("bef_lob", "string", "bef_lob", "string"),
        ("aft_lob", "string", "aft_lob", "string"),
        ("bef_value_amt", "double", "bef_value_amt", "double"),
        ("month_amt_ocr", "double", "month_amt_ocr", "double"),
        ("ord_typ", "string", "ord_typ", "string"),
        ("cable_up", "string", "cable_up", "string"),
        ("internet_up", "string", "internet_up", "string"),
        ("phone_up", "string", "phone_up", "string"),
        ("cable_down", "string", "cable_down", "string"),
        ("internet_down", "string", "internet_down", "string"),
        ("phone_down", "string", "phone_down", "string"),
        ("bef_video", "long", "bef_video", "long"),
        ("bef_hsd", "long", "bef_hsd", "long"),
        ("bef_voice", "long", "bef_voice", "long"),
        ("aft_video", "long", "aft_video", "long"),
        ("aft_hsd", "long", "aft_hsd", "long"),
        ("aft_voice", "long", "aft_voice", "long"),
        ("tenure", "long", "tenure", "long"),
        ("tc_count", "long", "tc_count", "long"),
        ("cit_calls_count", "long", "cit_calls_count", "long"),
        ("adjustment_count", "long", "adjustment_count", "long"),
        ("cs", "string", "cs", "string"),
        ("vol_disco_rsn", "string", "vol_disco_rsn", "string"),
        ("system", "string", "system", "string"),
        ("city", "string", "city", "string"),
        ("node", "string", "node", "string"),
        ("map_cde", "string", "map_cde", "string"),
        ("salesrep_area", "string", "salesrep_area", "string"),
        ("multiple_np", "long", "multiple_np", "long"),
        ("oper_area", "string", "oper_area", "string"),
        ("stb_sc", "string", "stb_sc", "string"),
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
pre_query += f"delete from prod.public.transactions_orderactivity where ls_chg_dte_ocr='{max_partition}';"
pre_query += "end;"

my_conn_options = {
    "dbtable": "transactions_orderactivity",
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
