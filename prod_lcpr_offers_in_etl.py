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
    df = df.withColumn("account_id", F.concat(F.lit("LCPR_FX_"), F.col("sub_acct_no")))
    

    df = df.withColumn("dt_ms", F.unix_timestamp(F.to_timestamp("dt", "d/M/yyyy")) * 1000)
    df = df.withColumn("next_best_action_date_ms", F.unix_timestamp(F.to_timestamp("next_best_action_date", "d/M/yyyy")) * 1000)
    

    sdf = DynamicFrame.fromDF(df, glueContext, "")
    return DynamicFrameCollection({"CustomTransform0": sdf}, glueContext)


DataCatalogtable_node_toa = glueContext.create_dynamic_frame.from_catalog(
    database="db_dev_cdp_project",
    table_name="lcpr_offers_in"
)

DataCatalogtable_node_toa.toDF().createOrReplaceTempView("df")

if ('--{}'.format('max_partition') in sys.argv):
    maxp = getResolvedOptions(sys.argv, ['max_partition'])
else:
    #q_max_date = """select max(ls_chg_dte_ocr) from df;"""
    #max_partition = spark.sql(q_max_date).collect()[0][0]

    maxp = {'max_partition': (datetime.today()-timedelta(days=1)).strftime('%Y-%m-%d')}
    
max_partition = maxp['max_partition']


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
        ("sub_acct_no", "string", "sub_acct_no", "string"),
        ("pkg_cde", "string", "pkg_cde", "string"),
        ("online_descr_pkg", "string", "online_descr_pkg", "string"),
        ("smt_descr_pkg", "string", "smt_descr_pkg", "string"),
        ("hsd_service", "string", "hsd_service", "string"),
        ("bundlecharge_csg", "double", "bundlecharge_csg", "double"),
        ("recommendedpkg", "string", "recommendedpkg", "string"),
        ("tocsgcodefriendlyname", "string", "tocsgcodefriendlyname", "string"),
        ("to_online_descr_pkg", "string", "to_online_descr_pkg", "string"),
        ("to_smt_descr_pckg", "string", "to_smt_descr_pckg", "string"),
        ("to_bundlecharge_csg", "string", "to_bundlecharge_csg", "string"),
        ("discount", "string", "discount", "string"),
        ("date", "string", "dt", "string"),
        ("rank", "string", "rank", "int"),
        ("pkg_type", "string", "pkg_type", "string"),
        ("stb", "string", "stb", "string"),
        ("additional_charge", "double", "additional_charge", "double"),
        ("delta_arpu", "string", "delta_arpu", "string"),
        ("source", "string", "source", "string"),
        ("regime", "string", "regime", "string"),
        ("reward", "double", "reward", "double"),
        ("use_case", "string", "use_case", "string"),
        ("offer_type", "string", "offer_type", "string"),
        ("channel", "string", "channel", "string"),
        ("week_day", "string", "week_day", "string"),
        ("next_best_action_date", "date", "next_best_action_date", "string"),
        ("time_frame", "string", "time_frame", "string"),
        ("additional_param_1", "string", "additional_param_1", "string"),
        ("additional_param_2", "string", "additional_param_2", "string"),
        ("additional_param_3", "string", "additional_param_3", "string"),
        ("additional_param_4", "string", "additional_param_4", "string"),
        ("additional_param_5", "string", "additional_param_5", "string"),
        ("additional_param_6", "string", "additional_param_6", "string"),
        ("additional_param_7", "string", "additional_param_7", "string"),
        ("additional_param_8", "string", "additional_param_8", "string"),
        ("additional_param_9", "string", "additional_param_9", "string"),
        ("additional_param_10", "string", "additional_param_10", "string"),
        ("id_oferta_int", "int", "id_oferta_int", "int")
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
pre_query += f"delete from prod.public.lcpr_offers_in where 1=1;"
pre_query += "end;"

my_conn_options = {
    "dbtable": "lcpr_offers_in",
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
