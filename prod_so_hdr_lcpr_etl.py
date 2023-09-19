import sys
from datetime import date, timedelta, datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql import functions as F, Window
    from pyspark.sql.types import StringType

    df = dfc.select(list(dfc.keys())[0]).toDF()
    df = df.withColumn("account_id", F.concat(F.lit("LCPR_FX_"), F.col("account_id")))
    
    for col, dtype in df.dtypes:
        if dtype == 'timestamp':
            df = df.withColumn(col, F.unix_timestamp(F.col(col).cast("timestamp")) * 1000)

    df = df.withColumn("dt_ms", F.unix_timestamp(F.col("dt").cast("timestamp")) * 1000)
    
    sdf = DynamicFrame.fromDF(df, glueContext, "")

    return DynamicFrameCollection({"CustomTransform0": sdf}, glueContext)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

if ('--{}'.format('max_partition') in sys.argv):
    maxp = getResolvedOptions(sys.argv, ['max_partition'])
else:
    maxp = {'max_partition': (datetime.today()-timedelta(days=1)).strftime('%Y-%m-%d')}
    
max_partition = maxp['max_partition']

print("filto a aplicar: ", max_partition) 

DataCatalogtable_node_so_hdr = glueContext.create_dynamic_frame.from_catalog(
    database="db-stage-prod",
    table_name="so_hdr_lcpr",
    additional_options = {
        "catalogPartitionPredicate":f"dt = '{max_partition}'"
    }
)

DropDuplicates_node_so = DynamicFrame.fromDF(
    DataCatalogtable_node_so_hdr.toDF().dropDuplicates(),
    glueContext
)

ApplyMapping_node_so = ApplyMapping.apply(
    frame=DropDuplicates_node_so,
    mappings=[
        ("org_id", "string", "org_id", "string"),
        ("org_cntry", "string", "org_cntry", "string"),
        ("order_id", "string", "order_id", "string"),
        ("order_start_date", "timestamp", "order_start_date", "timestamp"),
        ("completed_date", "timestamp", "completed_date", "timestamp"),
        ("cancelled_date", "timestamp", "cancelled_date", "timestamp"),
        ("order_status", "string", "order_status", "string"),
        ("sales_rep_id", "string", "sales_rep_id", "string"),
        ("sales_rep_name", "string", "sales_rep_name", "string"),
        ("csr_id", "string", "csr_id", "string"),
        ("csr_name", "string", "csr_name", "string"),
        ("channel_identifier", "string", "channel_identifier", "string"),
        ("channel_desc", "string", "channel_desc", "string"),
        ("channel_type", "string", "channel_type", "string"),
        ("channel_sub_type", "string", "channel_sub_type", "string"),
        ("order_contact_name", "string", "order_contact_name", "string"),
        ("order_contact_details", "string", "order_contact_details", "string"),
        ("order_location", "string", "order_location", "string"),
        ("order_type", "string", "order_type", "string"),
        ("network_type", "string", "network_type", "string"),
        ("appointment_required", "string", "appointment_required", "string"),
        ("lob_vo_count", "double", "lob_vo_count", "double"),
        ("lob_bb_count", "double", "lob_bb_count", "double"),
        ("lob_tv_count", "double", "lob_tv_count", "double"),
        ("lob_other_count", "double", "lob_other_count", "double"),
        ("cease_reason_code", "string", "cease_reason_code", "string"),
        ("cease_reason_desc", "string", "cease_reason_desc", "string"),
        ("cease_reason_group", "string", "cease_reason_group", "string"),
        ("cancel_reason_code", "string", "cancel_reason_code", "string"),
        ("cancel_reason_desc", "string", "cancel_reason_desc", "string"),
        ("currency_code", "string", "currency_code", "string"),
        ("estimated_rc_amt_local", "double", "estimated_rc_amt_local", "double"),
        ("estimated_nrc_amt_local", "double", "estimated_nrc_amt_local", "double"),
        ("account_id", "long", "account_id", "long"),
        ("account_name", "string", "account_name", "string"),
        ("account_type", "string", "account_type", "string"),
        ("customer_id", "long", "customer_id", "long"),
        ("source_system_name", "string", "source_system_name", "string"),
        ("data_creation_timestamp","timestamp","data_creation_timestamp","timestamp"),
        ("run_timestamp", "timestamp", "run_timestamp", "timestamp"),
        ("run_id", "int", "run_id", "int"),
        ("cust_id_type", "string", "cust_id_type", "string"),
        ("cust_id_value", "string", "cust_id_value", "string"),
        ("action_date", "timestamp", "action_date", "timestamp"),
        ("dt", "string", "dt", "string"),
        ("hr", "string", "hr", "string"),
    ]
)

CustomTransform_node_so = MyTransform(
    glueContext,
    DynamicFrameCollection({"ApplyMapping_node_so": ApplyMapping_node_so}, glueContext),
)

SelectFromCollection_node_so = SelectFromCollection.apply(
    dfc=CustomTransform_node_so,
    key=list(CustomTransform_node_so.keys())[0]
)

pre_query = "begin;"
pre_query += f"delete from prod.public.so_hdr_lcpr where dt='{max_partition}';"
pre_query += "end;"

my_conn_options = {
    "dbtable": "so_hdr_lcpr",
    "database": "prod",
    "aws_iam_role": "arn:aws:iam::283229902738:role/service-role/AmazonRedshift-CommandsAccessRole-20230522T161159",
    "preactions": pre_query
}

glueContext.write_dynamic_frame.from_jdbc_conf(
    frame = SelectFromCollection_node_so, 
    catalog_connection = "cdp-lcpr", 
    connection_options = my_conn_options, 
    redshift_tmp_dir = args["TempDir"]
)

job.commit()
