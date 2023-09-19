# inserta en redshift los datos de db-stage-prod.insights_customer_services_rates_lcpr 
# filtrados por dt = --max_partition
# siendo --max_partition calculado por la step function que lo invoca
# en caso de no recibir el parámetro esperado toma como valor por default date()-1


import sys
from datetime import date, timedelta, datetime
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
    df = df.withColumn("account_id", F.concat(F.lit("LCPR_FX_"), F.col("sub_acct_no_sbb")))
    
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

# compruebo si recibo max_partition en la invocación, caso contrario asigno valor por default

if ('--{}'.format('max_partition') in sys.argv):
    maxp = getResolvedOptions(sys.argv, ['max_partition'])
else:
    maxp = {'max_partition': (datetime.today()-timedelta(days=1)).strftime('%Y-%m-%d')}
    
max_partition = maxp['max_partition']

print("filto a aplicar: ", max_partition) 


DataCatalogtable_node_csr = glueContext.create_dynamic_frame.from_catalog(
    database="db-stage-prod",
    table_name="insights_customer_services_rates_lcpr",
     additional_options = {
        "catalogPartitionPredicate":f"dt = '{max_partition}'"
    }
)

DropDuplicates_node_csr = DynamicFrame.fromDF(
    DataCatalogtable_node_csr.toDF().dropDuplicates(),
    glueContext
)

ApplyMapping_node_csr = ApplyMapping.apply(
    frame=DropDuplicates_node_csr,
    mappings=[
        ("as_of", "timestamp", "as_of", "timestamp"),
        ("prin_sbb", "int", "prin_sbb", "int"),
        ("agnt_sbb", "int", "agnt_sbb", "int"),
        ("sub_acct_no_sbb", "long", "sub_acct_no_sbb", "long"),
        ("res_name_sbb", "string", "res_name_sbb", "string"),
        ("home_phone_sbb", "long", "home_phone_sbb", "long"),
        ("bus_phone_sbb", "long", "bus_phone_sbb", "long"),
        ("vip_flg_sbb", "string", "vip_flg_sbb", "string"),
        ("cyc_cde_sbb", "int", "cyc_cde_sbb", "int"),
        ("cust_typ_sbb", "string", "cust_typ_sbb", "string"),
        ("cur_bal_sbb", "double", "cur_bal_sbb", "double"),
        ("connect_dte_sbb", "timestamp", "connect_dte_sbb", "timestamp"),
        ("email", "string", "email", "string"),
        ("cs", "string", "cs", "string"),
        ("tenure", "double", "tenure", "double"),
        ("addr1_hse", "string", "addr1_hse", "string"),
        ("res_addr_2_hse", "string", "res_addr_2_hse", "string"),
        ("res_city_hse", "string", "res_city_hse", "string"),
        ("postal_code", "int", "postal_code", "int"),
        ("map_cde_hse", "string", "map_cde_hse", "string"),
        ("bridger_addr_hse", "string", "bridger_addr_hse", "string"),
        ("video", "int", "video", "int"),
        ("video_tier", "string", "video_tier", "string"),
        ("primary_video_services", "string", "primary_video_services", "string"),
        ("add_video_services", "string", "add_video_services", "string"),
        ("hsd", "int", "hsd", "int"),
        ("hsd_speed", "string", "hsd_speed", "string"),
        ("hsd_upload_speed", "string", "hsd_upload_speed", "string"),
        ("voice", "int", "voice", "int"),
        ("play_type", "string", "play_type", "string"),
        ("hub_tv", "int", "hub_tv", "int"),
        ("cable_cards", "int", "cable_cards", "int"),
        ("dta", "int", "dta", "int"),
        ("dvr", "int", "dvr", "int"),
        ("hd", "int", "hd", "int"),
        ("sd", "int", "sd", "int"),
        ("cpe", "string", "cpe", "string"),
        ("video_chrg", "double", "video_chrg", "double"),
        ("hsd_chrg", "double", "hsd_chrg", "double"),
        ("voice_chrg", "double", "voice_chrg", "double"),
        ("bill_code", "string", "bill_code", "string"),
        ("drop_type", "string", "drop_type", "string"),
        ("stb_chrg", "double", "stb_chrg", "double"),
        ("bill_from_dte_sbb", "timestamp", "bill_from_dte_sbb", "timestamp"),
        ("delq_stat_sbb", "string", "delq_stat_sbb", "string"),
        ("joint_customer", "string", "joint_customer", "string"),
        ("welcome_offer", "string", "welcome_offer", "string"),
        ("wireless_cust_id_pstpd", "string", "wireless_cust_id_pstpd", "string"),
        ("wireless_cust_id_prepd", "string", "wireless_cust_id_prepd", "string"),
        ("ls_pay_dte_sbb", "timestamp", "ls_pay_dte_sbb", "timestamp"),
        ("ls_pay_amt_sbb", "double", "ls_pay_amt_sbb", "double"),
        ("delinquency_days", "int", "delinquency_days", "int"),
        ("delinquency_amt", "double", "delinquency_amt", "double"),
        ("acp", "string", "acp", "string"),
        ("dt", "string", "dt", "string"),
    ]
)

CustomTransform_node_csr = MyTransform(
    glueContext,
    DynamicFrameCollection(
        {"ApplyMapping_node_csr":ApplyMapping_node_csr}, glueContext
    ),
)

SelectFromCollection_node_csr = SelectFromCollection.apply(
    dfc=CustomTransform_node_csr,
    key=list(CustomTransform_node_csr.keys())[0]
)

pre_query = "begin;"
pre_query += f"delete from prod.public.insights_customer_services_rates_lcpr where dt='{max_partition}';"
pre_query += "end;"

my_conn_options = {
    "dbtable": "insights_customer_services_rates_lcpr",
    "database": "prod",
    "aws_iam_role": "arn:aws:iam::283229902738:role/service-role/AmazonRedshift-CommandsAccessRole-20230522T161159",
    "preactions": pre_query
}

glueContext.write_dynamic_frame.from_jdbc_conf(
    frame = SelectFromCollection_node_csr, 
    catalog_connection = "cdp-lcpr", 
    connection_options = my_conn_options, 
    redshift_tmp_dir = args["TempDir"]
)

job.commit()
