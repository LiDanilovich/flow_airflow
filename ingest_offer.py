from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from datetime import date, timedelta, datetime
import sys, os, re
import json
import boto3
from CDPPy.autoexec_glue import *
from CDPPy.job_functions import create_partition_v2

print("STARTING ...")
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'bucket', 'prefix', 'key', 'method'])
job.init(args['JOB_NAME'], args)

bucket = args['bucket']
prefix = args['prefix']
key = args['key']
method = args['method']

today = (datetime.today()-timedelta(days=0)).strftime('%Y%m%d %H%M%S')
today_year=today[0:4]
today_month=str(today)[4:6]
today_day=str(today)[6:8]
hour_min_sec = str(today)[9:15]


def extract_date_from_mask(mask):
    # Expresión regular para buscar cuatro dígitos seguidos (para el año),
    # seguidos de cualquier número de caracteres no dígitos,
    # seguidos de dos dígitos (para el mes),
    # seguidos de cualquier número de caracteres no dígitos,
    # seguidos de dos dígitos (para el día)
    match = re.search(r'(\d{4})\D+(\d{2})\D+(\d{2})', mask)

    if match:
        year, month, day = match.groups()
        return year, month, day
    else:
        return None, None, None

def extract_date_parts(key):
    match = re.search(r'(\d{4})(\d{2})(\d{2})', key)

    if match:
        year, month, day = match.groups()
        return year, month, day
    else:
        return None, None, None

def ingest_offers_links_input_function(prefix, key, bucket):
    #Creates a list of files
    s3_resource = boto3.resource('s3')
    publibucket = s3_resource.Bucket(str(bucket))
    objects = publibucket.objects.filter(Prefix = str(prefix))
    objects_to_read = str(key)
    year, month, day = extract_date_from_mask(key)
    theFile = 's3://' + str(bucket) + '/' + str(prefix) + str(objects_to_read)
    mask_file= 'lcpr_offers_links_input_'
    print('publibucket: '+ str(publibucket))
    print('objects: '+ str(objects))
    print('objects_to_read: '+ str(objects_to_read))
    print('fecha archivo: '+ str(year)+str(month)+str(day))
    print('theFile: '+ str(theFile))

    input_offerfit_file = spark.read.option("header","true").option("delimiter", ",").csv(theFile)
    input_offerfit_file.createOrReplaceTempView("input_offerfit_file")
    print("The original df has ", str(input_offerfit_file.count()), " rows ")

    input_offerfit_df = spark.sql(f"""
    select 
    cast(AIQ_EXPORTID as string) as AIQ_EXPORTID,
    cast(AIQ_SEGMENTID as string) as AIQ_SEGMENTID,
    cast(ORG_CNTRY as string) as ORG_CNTRY,	  
    cast(LCPR_CSR_CUST_ACCT as bigint) as LCPR_CSR_CUST_ACCT,
    cast(LCPR_OFFER_EMAIL_NEXT_BST_ACTION_DTE as bigint) as LCPR_OFFER_EMAIL_NEXT_BST_ACTION_DTE,
    cast(LCPR_EXPIRATION_DATE as bigint) as LCPR_EXPIRATION_DATE,	       
    cast(CHANNEL as string) as CHANNEL, 
    cast(USE_CASE as string) as USE_CASE,
    cast(OFFER_TYPE as string) as OFFER_TYPE
    from input_offerfit_file
    """)
    
    input_offerfit_df = input_offerfit_df.withColumn("LCPR_OFFER_EMAIL_NEXT_BST_ACTION_DTE", F.to_date(F.from_unixtime(F.col("LCPR_OFFER_EMAIL_NEXT_BST_ACTION_DTE") / 1000)))
    input_offerfit_df = input_offerfit_df.withColumn("LCPR_EXPIRATION_DATE", F.to_date(F.from_unixtime(F.col("LCPR_EXPIRATION_DATE") / 1000)))
    
    input_offerfit_df.show(10, truncate=False)
    input_offerfit_df.createOrReplaceTempView("inp_offerfit_df")
    print("The input_offerfit df has ", str(input_offerfit_df.count()), " rows ")

    athena_view_dataframe = (
        glueContext.read.format("jdbc")
        .option("driver", "com.simba.athena.jdbc.Driver")
        .option("AwsCredentialsProviderClass","com.simba.athena.amazonaws.auth.InstanceProfileCredentialsProvider")
        .option("url", "jdbc:awsathena://athena.us-east-1.amazonaws.com:443")
        .option("dbtable", "AwsDataCatalog.db_dev_cdp_project.v_lcpr_offers")
        .option("S3OutputLocation","s3://cdp-athena-query-results/")
        .load()
    )

    athena_view_dataframe.printSchema()
    athena_view_dataframe.createOrReplaceTempView("v_lcpr_offers")
    spark.sql(""" select * from v_lcpr_offers limit 3 """).show(3, truncate=False)

    input_offerfit_fin_df = spark.sql(f"""
    select t1.sub_acct_no AS SUB_ACCT_NO_SBB , 
    t1.PKG_CDE AS FROM_CSG_CODE, 
    t1.HSD_SERVICE AS FROM_CSG_CODE_DESCRIPTION, 
    t1.RECOMMENDEDPKG AS TO_CSG_CODE, 
    t1.TOCSGCODEFRIENDLYNAME AS TO_CSG_CODE_DESCRIPTION, 
    t1.DELTA_ARPU AS PAYMENT_DIF,
    t2.LCPR_EXPIRATION_DATE as LINK_EXPIRATION_DT,
    t1.ID_OFERTA_INT,
    '{str(year)+str(month)}' as month,
    '{str(year)+str(month)+str(day)}' as day,
    '{str(year)+str(month)+str(day)+str(hour_min_sec)}' as day_hour
    from v_lcpr_offers t1
    inner join inp_offerfit_df t2
    on t1.sub_acct_no = t2.lcpr_csr_cust_acct 
    and t1.channel = t2.channel
    and cast(t1.next_best_action_date as string) = cast(t2.lcpr_offer_email_next_bst_action_dte as string)
    and t1.use_case = t2.use_case
    and t1.offer_type = t2.offer_type
    """)

    input_offerfit_fin_df.createOrReplaceTempView("hist_input_offerfit_in")
    print("The input_offerfit_fin_df has ", str(input_offerfit_fin_df.count()), " rows ")
    s3path = "s3://cdp-lcpr-process/hist/hist_offer_in"
    input_offerfit_fin_df = input_offerfit_fin_df.repartition("month", "day", "day_hour")

    input_offerfit_fin_df \
        .write.mode('overwrite') \
        .format('parquet') \
        .partitionBy('month', 'day', 'day_hour') \
        .save( s3path )	

    create_partition_v2('db_dev_cdp_project', 'hist_offer_in', str(year)+str(month)+str(day)+str(hour_min_sec))

    input_offerfit_fin_df.show(10, truncate=False)
    input_offerfit_fin_df.createOrReplaceTempView("input_offerfit_fin_df")
    print("The input_offerfit_fin df has ", str(input_offerfit_fin_df.count()), " rows ")

    s3path2 = "s3://cdp-lcpr-process/offers/csv_in/"

    input_offerfit_fin_df = input_offerfit_fin_df.drop(col("id_oferta_int")).drop(col("month")).drop(col("day")).drop(col("day_hour")).repartition(1)
    input_offerfit_fin_df \
        .write.mode('append') \
        .format('csv') \
        .option("header","true") \
        .option("ignoreTrailingWhiteSpace","true") \
        .option("ignoreLeadingWhiteSpace","true") \
        .save( s3path2 )

    publibucket = s3_resource.Bucket(str(bucket))
    objects = publibucket.objects.filter(Prefix = 'offers/csv_in/')
    objects_to_read = [o.key.split('/')[-1] for o in objects if o.key.endswith('.csv') and 'part' in o.key][0]
    theobject= str(bucket)+'/offers/csv_in/'+objects_to_read
    thekey= 'offers/csv_in/'+ objects_to_read
    writtenobject = 'bi_links/in/'+ mask_file+today_year+today_month+today_day+hour_min_sec+".csv"

    s3_resource.Object('cdp-lcpr-transfer-offers-bucket', writtenobject).copy_from(CopySource= theobject)
    s3_resource.Object('cdp-lcpr-process',thekey).delete()

    # 	s3_resource = boto3.resource('s3')
    # 	publibucket = s3_resource.Bucket(bucket)
    # 	objects = publibucket.objects.filter(Prefix = prefix)
    # 	objects_to_copy = [{'Key': o.key} for o in objects if o.key.endswith('.csv') and year+month+day in o.key]
    # 	print("The objects to copy are : ", objects_to_copy)

    query = spark.sql("""MSCK REPAIR TABLE db_dev_cdp_project.hist_offer_in""")
    print('Actualiza: db_dev_cdp_project.hist_offer_in')

def ingest_offers_links_output_function(prefix, key, bucket):
    #Creates a list of files
    s3_resource = boto3.resource('s3')
    publibucket = s3_resource.Bucket(str(bucket))
    objects = publibucket.objects.filter(Prefix = str(prefix))
    objects_to_read = str(key)
    theFile = 's3://' + str(bucket) + '/' + str(prefix) + str(objects_to_read)
    year, month, day = extract_date_parts(key)
    mask_file = 'lcpr_offers_links_output_'

    links_output_bi_file = spark.read.option("header","true").option("delimiter", ",").csv(theFile)
    links_output_bi_file.createOrReplaceTempView("links_output_bi_file")
    print("The original df has ", str(links_output_bi_file.count()), " rows ")

    links_output_bi_df = spark.sql(f"""
    select 
    cast(SUB_ACCT_NO as bigint) as SUB_ACCT_NO,
    cast(LINK as string) as LINK,	  
    cast(CREATED_DATETIME as string) as LINK_GENERATED_DT,
    cast(LINK_EXPIRATION_DT as string) as LINK_EXPIRATION_DT
    from links_output_bi_file
    """)
    
    datetime_format = "M/d/yyyy h:mm:ss a"
    date_format = "yyyy-MM-dd"
    links_output_bi_df = links_output_bi_df.withColumn("LINK_GENERATED_DT", F.to_date(F.col("LINK_GENERATED_DT"), datetime_format))
    links_output_bi_df = links_output_bi_df.withColumn("LINK_EXPIRATION_DT", F.to_date(F.col("LINK_EXPIRATION_DT"), date_format))

    links_output_bi_df.show(10, truncate=False)
    links_output_bi_df.createOrReplaceTempView("links_output_bi_df")
    print("The links_output_bi_df df has ", str(links_output_bi_df.count()), " rows ")

    links_output_bi_join_offer_df = spark.sql(f"""
    with link_out_df as 
    (SELECT 
    t1.SUB_ACCT_NO_SBB, 
    t3.LINK, 
    t3.LINK_GENERATED_DT , 
    t3.LINK_EXPIRATION_DT,
    t1.id_oferta_int,
    '{str(year)+str(month)}' as month,
    '{str(year)+str(month)+str(day)}' as day,
    '{str(year)+str(month)+str(day)+str(hour_min_sec)}' as day_hour,
    row_number() over (partition by t1.SUB_ACCT_NO_SBB order by t1.day_hour desc) as last_partition
    FROM db_dev_cdp_project.hist_offer_in t1
    INNER JOIN links_output_bi_df t3
    on t1.SUB_ACCT_NO_SBB = t3.SUB_ACCT_NO
    and to_date(t1.day, 'yyyyMMdd') = t3.LINK_GENERATED_DT
    )
    SELECT
    *
    FROM link_out_df
    WHERE last_partition = 1
    """) 
    
    links_output_bi_join_offer_df = links_output_bi_join_offer_df.drop(col("last_partition"))

    # links_output_bi_join_offer_df.createOrReplaceTempView("hist_output_bi_links")
    print("The links_output_bi_join_offer_df has ", str(links_output_bi_join_offer_df.count()), " rows ")
    s3path = "s3://cdp-lcpr-process/hist/hist_links_output"
    links_output_bi_join_offer_df = links_output_bi_join_offer_df.repartition("month", "day", "day_hour")

    links_output_bi_join_offer_df \
        .write.mode('overwrite') \
        .format('parquet') \
        .partitionBy('month', 'day', 'day_hour') \
        .save( s3path )	

    create_partition_v2('db_dev_cdp_project', 'hist_links_output', str(year)+str(month)+str(day)+str(hour_min_sec))

    query = spark.sql("""MSCK REPAIR TABLE db_dev_cdp_project.hist_links_output""")
    print('Actualiza: db_dev_cdp_project.hist_links_output')

def ingest_rpp_function(prefix, key, bucket):
    #Creates a list of files
    s3_resource = boto3.resource('s3')
    publibucket = s3_resource.Bucket(str(bucket))
    objects = publibucket.objects.filter(Prefix = str(prefix))
    objects_to_read = str(key)
    year, month, day = extract_date_from_mask(key)
    theFile = 's3://' + str(bucket) + '/' + str(prefix) + str(objects_to_read)
    mask_file = 'lcpr_offers_rpp_input_'

    input_rpp_file = spark.read.option("header","true").option("delimiter", ",").csv(theFile)
    input_rpp_file.createOrReplaceTempView("input_rpp_file")
    print("The original df has ", str(input_rpp_file.count()), " rows ")

    input_rpp_df = spark.sql(f"""
    select 
    cast(AIQ_EXPORTID as string) as AIQ_EXPORTID,
    cast(AIQ_SEGMENTID as string) as AIQ_SEGMENTID,	    
    cast(ORG_CNTRY as string) as ORG_CNTRY,
    cast(LCPR_CSR_CUST_ACCT as bigint) as LCPR_CSR_CUST_ACCT,
    cast(OFFER_TYPE as string) as OFFER_TYPE
    from input_rpp_file
    """)

    athena_view_dataframe = (
        glueContext.read.format("jdbc")
        .option("driver", "com.simba.athena.jdbc.Driver")
        .option("AwsCredentialsProviderClass","com.simba.athena.amazonaws.auth.InstanceProfileCredentialsProvider")
        .option("url", "jdbc:awsathena://athena.us-east-1.amazonaws.com:443")
        .option("dbtable", "AwsDataCatalog.db_dev_cdp_project.v_lcpr_offers")
        .option("S3OutputLocation","s3://cdp-athena-query-results/")
        .load()
    )

    athena_view_dataframe.printSchema()
    athena_view_dataframe.createOrReplaceTempView("v_lcpr_offers")
    spark.sql(""" select * from v_lcpr_offers limit 3 """).show(3, truncate=False)

    input_rpp_df.show(10, truncate=False)
    input_rpp_df.createOrReplaceTempView("input_rpp_df")
    print("The input_rpp_df df has ", str(input_rpp_df.count()), " rows ")

    rpp_join_offer_df = spark.sql(f"""
    SELECT t1.SUB_ACCT_NO 
    ,t1.PKG_CDE
    ,t1.ONLINE_DESCR_PKG
    ,t1.SMT_DESCR_PKG
    ,t1.HSD_SERVICE
    ,t1.BUNDLECHARGE_CSG
    ,t1.RECOMMENDEDPKG
    ,t1.DISCOUNT
    ,t1.DATE
    ,t1.RANK
    ,t1.PKG_TYPE
    ,t1.STB
    ,t1.ADDITIONAL_CHARGE
    ,t1.DELTA_ARPU
    ,t1.REGIME
    ,t1.REWARD,
    '{str(year)+str(month)}' as month,
    '{str(year)+str(month)+str(day)}' as day,
    '{str(year)+str(month)+str(day)+str(hour_min_sec)}' as day_hour
    FROM v_lcpr_offers T1
    INNER JOIN input_rpp_df T2
    on t1.SUB_ACCT_NO = t2.LCPR_CSR_CUST_ACCT 
    and t1.offer_type = t2.offer_type
    """)

    rpp_join_offer_df.createOrReplaceTempView("hist_rpp")
    print("The hist_rpp has ", str(rpp_join_offer_df.count()), " rows ")
    s3path = "s3://cdp-lcpr-process/hist/hist_rpp"
    hist_rpp = rpp_join_offer_df.repartition("month", "day", "day_hour")

    hist_rpp \
        .write.mode('overwrite') \
        .format('parquet') \
        .partitionBy('month', 'day', 'day_hour') \
        .save( s3path )	

    create_partition_v2('db_dev_cdp_project', 'hist_rpp', str(year)+str(month)+str(day)+str(hour_min_sec))

    s3path2 = "s3://cdp-lcpr-process/offers/csv_in/"
    rpp_join_offer_df = rpp_join_offer_df.withColumn("REWARD",format_number("REWARD",15))
    rpp_join_offer_df = rpp_join_offer_df.drop(col("month")).drop(col("day")).drop(col("day_hour")).repartition(1)
    
    rpp_join_offer_df.printSchema()
    rpp_join_offer_df.show(10, truncate=False)
    
    rpp_join_offer_df \
        .write.mode('append') \
        .format('csv') \
        .option("header","true") \
        .option("ignoreTrailingWhiteSpace","true") \
        .option("ignoreLeadingWhiteSpace","true") \
        .save( s3path2 )

    publibucket = s3_resource.Bucket(str(bucket))
    objects = publibucket.objects.filter(Prefix = 'offers/csv_in/')
    objects_to_read = [o.key.split('/')[-1] for o in objects if o.key.endswith('.csv') and 'part' in o.key][0]
    theobject= str(bucket)+'/offers/csv_in/'+objects_to_read
    thekey= 'offers/csv_in/'+ objects_to_read
    writtenobject = 'rpp/in/'+ mask_file+today_year+today_month+today_day+hour_min_sec+".csv"

    s3_resource.Object('cdp-lcpr-transfer-offers-bucket', writtenobject).copy_from(CopySource= theobject)
    s3_resource.Object('cdp-lcpr-process',thekey).delete()

    # 	s3_resource = boto3.resource('s3')
    # 	publibucket = s3_resource.Bucket(bucket)
    # 	objects = publibucket.objects.filter(Prefix = prefix)
    # 	objects_to_copy = [{'Key': o.key} for o in objects if o.key.endswith('.csv') and year+month+day in o.key]
    # 	print("The objects to copy are : ", objects_to_copy)

    query = spark.sql("""MSCK REPAIR TABLE db_dev_cdp_project.hist_rpp""")
    print('Actualiza: db_dev_cdp_project.hist_rpp')

if method == 'links_in':
    ingest_offers_links_input_function(prefix, key, bucket)
elif method == 'links_out':
    ingest_offers_links_output_function(prefix, key, bucket)
elif method == 'rpp_in':
    ingest_rpp_function(prefix, key, bucket)

job.commit()