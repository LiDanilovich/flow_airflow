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

DataCatalogtable_int = glueContext.create_dynamic_frame.from_catalog(
    database="db-stage-prod",
    table_name="interactions_lcpr",
    additional_options = {
        "catalogPartitionPredicate":f"dt = '{max_partition}'"
    }
)

DropDuplicates_int = DynamicFrame.fromDF(
    DataCatalogtable_int.toDF().dropDuplicates(),
    glueContext
)

ApplyMapping_int = ApplyMapping.apply(
    frame=DropDuplicates_int,
    mappings=[
        ("interaction_id", "string", "interaction_id", "string"),
        ("interaction_start_time", "timestamp", "interaction_start_time", "timestamp"),
        ("interaction_end_time", "timestamp", "interaction_end_time", "timestamp"),
        ("interaction_channel", "string", "interaction_channel", "string"),
        ("interaction_phone_no", "string", "interaction_phone_no", "string"),
        ("interaction_agent_id", "string", "interaction_agent_id", "string"),
        ("interaction_agent_name", "string", "interaction_agent_name", "string"),
        ("interaction_language", "string", "interaction_language", "string"),
        ("interaction_direction", "string", "interaction_direction", "string"),
        ("interaction_product_id", "string", "interaction_product_id", "string"),
        ("interaction_purpose_descrip","string","interaction_purpose_descrip","string"),
        ("interaction_status", "string", "interaction_status", "string"),
        ("other_interaction_info10", "string", "other_interaction_info10", "string"),
        ("interaction_voice_rec_id", "string", "interaction_voice_rec_id", "string"),
        ("interaction_web_log_detail","string","interaction_web_log_detail","string"),
        ("interaction_activity_log_dtl","string","interaction_activity_log_dtl","string"),
        ("survey_id", "string", "survey_id", "string"),
        ("survey_channel_id", "string", "survey_channel_id", "string"),
        ("customer_satisfaction_grade","string","customer_satisfaction_grade","string"),
        ("net_promoter_score", "double", "net_promoter_score", "double"),
        ("sentiment_result_description","string","sentiment_result_description","string"),
        ("issue_resolution_flag", "string", "issue_resolution_flag", "string"),
        ("abandonment_rate", "int", "abandonment_rate", "int"),
        ("other_interaction_info1", "string", "other_interaction_info1", "string"),
        ("customer_id", "string", "customer_id", "string"),
        ("source_system_name", "string", "source_system_name", "string"),
        ("data_creation_timestamp","timestamp","data_creation_timestamp","timestamp"),
        ("run_timestamp", "timestamp", "run_timestamp", "timestamp"),
        ("run_id", "long", "run_id", "long"),
        ("valid_from_timestamp", "timestamp", "valid_from_timestamp", "timestamp"),
        ("account_id", "string", "account_id", "string"),
        ("other_interaction_info2","timestamp","other_interaction_info2","timestamp"),
        ("other_interaction_info3","timestamp","other_interaction_info3","timestamp"),
        ("other_interaction_info4", "string", "other_interaction_info4", "string"),
        ("other_interaction_info5", "string", "other_interaction_info5", "string"),
        ("other_interaction_info6", "string", "other_interaction_info6", "string"),
        ("other_interaction_info7", "string", "other_interaction_info7", "string"),
        ("other_interaction_info8", "string", "other_interaction_info8", "string"),
        ("other_interaction_info9", "string", "other_interaction_info9", "string"),
        ("other_interaction_info11", "string", "other_interaction_info11", "string"),
        ("other_interaction_info12", "string", "other_interaction_info12", "string"),
        ("other_interaction_info13", "string", "other_interaction_info13", "string"),
        ("org_cntry", "string", "org_cntry", "string"),
        ("src_file_name", "string", "src_file_name", "string"),
        ("org_id", "string", "org_id", "string"),
        ("interaction_type_id", "string", "interaction_type_id", "string"),
        ("interaction_outcome_date","timestamp","interaction_outcome_date","timestamp"),
        ("account_type", "string", "account_type", "string"),
        ("dt", "string", "dt", "string")
    ]
)

CustomTransform_int = MyTransform(
    glueContext,
    DynamicFrameCollection({"ApplyMapping_int": ApplyMapping_int}, glueContext),
)

SelectFromCollection_int = SelectFromCollection.apply(
    dfc=CustomTransform_int,
    key=list(CustomTransform_int.keys())[0]
)

pre_query = "begin;"
pre_query += f"delete from prod.public.interactions_lcpr where dt='{max_partition}';"
pre_query += "end;"

my_conn_options = {
    "dbtable": "interactions_lcpr",
    "database": "prod",
    "aws_iam_role": "arn:aws:iam::283229902738:role/service-role/AmazonRedshift-CommandsAccessRole-20230522T161159",
    "preactions": pre_query
}

glueContext.write_dynamic_frame.from_jdbc_conf(
    frame = SelectFromCollection_int, 
    catalog_connection = "cdp-lcpr", 
    connection_options = my_conn_options, 
    redshift_tmp_dir = args["TempDir"]
)

job.commit()
