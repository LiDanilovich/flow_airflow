
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import date, timedelta, datetime
import json
import boto3
import pandas as pd

from CDPPy.autoexec_glue import *
from CDPPy.job_functions import create_partition_v2
from CDPPy.staging_queries import *

print("STARTING ...")

# el parámetro days_before se utiliza para setear el feedback de qué día voy a ingestar.   Por default es 1 (feedback de ayer)
if ('--{}'.format('days_before') in sys.argv):
    args = getResolvedOptions(sys.argv, ['days_before'])
else:
    args = {'days_before': '1'}
    
days_before = int(args['days_before'])

today = (datetime.today()-timedelta(days=days_before)).strftime('%Y-%m-%d')
print('buscamos feedback del ',today)

year=today[0:4]
month=str(today)[5:7]
day=str(today)[8:10]

session = boto3.session.Session()
client = session.client(
        service_name='secretsmanager',
        region_name='us-east-1')
        
client = session.client(service_name='secretsmanager', region_name='us-east-1')
creds = client.get_secret_value(SecretId="bigq_sap_od_sapodlilac")
service_account_info = json.loads(json.loads(creds['SecretString'], strict=False)['credentials'])

credentials = service_account.Credentials.from_service_account_info(service_account_info)

client = bigquery.Client(credentials=credentials)

spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

def ingest_feedback_email(mySql,mySchema,myPath):
    query_job = client.query(mySql)
    pandasDF = query_job.to_dataframe()
    sparkDF=spark.createDataFrame(pandasDF,schema=mySchema)
    
    sparkDF.createOrReplaceTempView("sparkDF")
    finalDF = spark.sql(f""" select *, 
             '{str(year)+str(month)}' as month,
             '{str(year)+str(month)+str(day)}' as day 
             from sparkDF """)

    finalDF.show(3, truncate=False)
    print("Cant.Registros=",(finalDF.count()))
    
    if finalDF.count() > 0:
       finalDF = finalDF.repartition("month", "day")

       finalDF \
         .write.mode('overwrite') \
         .format('parquet') \
         .partitionBy('month', 'day') \
         .save( myPath )
      
    return    
  


# def for email_campaigns

sql_campaigns = """ 
select id, name, version_name, language, category_id, parent_campaign_id, type, sub_type, program_id, partitiontime, event_time, loaded_at
from `sap-od-sapodlilac.emarsys_sapodlilac_837621947.email_campaigns_837621947` 
WHERE date(event_time) = '{}'
""".format(today)

schema_campaigns = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("version_name", StringType(), True),
    StructField("language", StringType(), True),
    StructField("category_id", IntegerType(), True),
    StructField("parent_campaign_id", LongType(), True) ,
    StructField("type", StringType(), True),
    StructField("sub_type", StringType(), True),
    StructField("program_id", LongType(), True),
    StructField("partitiontime", TimestampType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("loaded_at", TimestampType(), True)
    ])


s3path_campaigns = "s3://cdp-lcpr-process/em/feedback/campaigns"

# .................................................................................................................
# def for email sends

sql_sends = """
select contact_id, launch_id, campaign_type,  domain, campaign_id, message_id, event_time, customer_id, partitiontime, loaded_at
from `sap-od-sapodlilac.emarsys_sapodlilac_837621947.email_sends_837621947` 
WHERE contact_id is not null and  date(event_time) = '{}'
""".format(today)


schema_sends = StructType([
    StructField("contact_id", LongType(), True),
    StructField("launch_id", LongType(), True),
    StructField("campaign_type", StringType(), True),
    StructField("domain", StringType(), True),
    StructField("campaign_id", LongType(), True) ,
    StructField("message_id", IntegerType(), True) ,
    StructField("event_time", TimestampType(), True),
    StructField("customer_id", LongType(), True),
    StructField("partitiontime", TimestampType(), True),
    StructField("loaded_at", TimestampType(), True)
    ])


s3path_sends = "s3://cdp-lcpr-process/em/feedback/sends"


# .................................................................................................................

# def for email opens
sql_opens = """
select contact_id, launch_id, domain, email_sent_at, campaign_type, geo, platform, md5, is_mobile, is_anonymized, uid, ip, user_agent, generated_from, campaign_id, message_id, event_time, customer_id, partitiontime, loaded_at
from `sap-od-sapodlilac.emarsys_sapodlilac_837621947.email_opens_837621947` 
WHERE contact_id is not null and  date(event_time) = '{}'
""".format(today)

schema_opens = StructType([
    StructField("contact_id", LongType(), True),
    StructField("launch_id", LongType(), True),
    StructField("domain", StringType(), True),
    StructField("email_sent_at", TimestampType(), True),
    StructField("campaign_type", StringType(), True),
    StructField(
        "geo",
        StructType([
            StructField("postal_code", StringType(), True),
            StructField("latitude", StringType(), True),
            StructField("longitude", StringType(), True),
            StructField("accuracy_radius", StringType(), True),
            StructField("continent_code", StringType(), True),
            StructField("country_iso_code", StringType(), True),
            StructField("continent_code", StringType(), True),
            StructField("city_name", StringType(), True),
            StructField("time_zone", StringType(), True),
        ]),
        True
        ),
    StructField("platform", StringType(), True),
    StructField("md5", StringType(), True),
    StructField("is_mobile", StringType(), True),
    StructField("is_anonymized", StringType(), True),
    StructField("uid", StringType(), True),
    StructField("ip", StringType(), True),
    StructField("user_agent", StringType(), True),
    StructField("generated_from", StringType(), True),
    StructField("campaign_id", StringType(), True) ,
    StructField("message_id", StringType(), True) ,
    StructField("event_time", TimestampType(), True),
    StructField("customer_id", StringType(), True),
    StructField("partitiontime", TimestampType(), True),
    StructField("loaded_at", TimestampType(), True)
    ])


s3path_opens = "s3://cdp-lcpr-process/em/feedback/opens"

# .................................................................................................................
# def for email bounces

sql_bounces = """
select contact_id, launch_id,  domain, email_sent_at, campaign_type, bounce_type,
campaign_id, message_id, event_time, customer_id, partitiontime, loaded_at, dsn_reason
from `sap-od-sapodlilac.emarsys_sapodlilac_837621947.email_bounces_837621947` 
WHERE contact_id is not null and date(event_time) = '{}'
""".format(today)


schema_bounces = StructType([
    StructField("contact_id", LongType(), True),
    StructField("launch_id", LongType(), True),
    StructField("domain", StringType(), True),
    StructField("email_sent_at", TimestampType(), True),
    StructField("campaign_type", StringType(), True),
    StructField("bounce_type", StringType(), True),
    StructField("campaign_id", LongType(), True) ,
    StructField("message_id", IntegerType(), True) ,
    StructField("event_time", TimestampType(), True),
    StructField("customer_id", LongType(), True),
    StructField("partitiontime", TimestampType(), True),
    StructField("loaded_at", TimestampType(), True),
    StructField("dsn_reason", StringType(), True),
    ])


s3path_bounces = "s3://cdp-lcpr-process/em/feedback/bounces"

# .................................................................................................................
# def for email cancels

sql_cancels = """
select contact_id, launch_id, reason, campaign_type, suite_type, suite_event,
campaign_id, message_id, event_time, customer_id, partitiontime, loaded_at
from `sap-od-sapodlilac.emarsys_sapodlilac_837621947.email_cancels_837621947` 
WHERE contact_id is not null and date(event_time) = '{}'
""".format(today)


schema_cancels = StructType([
    StructField("contact_id", LongType(), True),
    StructField("launch_id", LongType(), True),
    StructField("reason", StringType(), True),
    StructField("campaign_type", StringType(), True),
    StructField("suite_type", StringType(), True),
    StructField("suite_event", StringType(), True),
    StructField("campaign_id", LongType(), True) ,
    StructField("message_id", IntegerType(), True) ,
    StructField("event_time", TimestampType(), True),
    StructField("customer_id", LongType(), True),
    StructField("partitiontime", TimestampType(), True),
    StructField("loaded_at", TimestampType(), True)
    ])


s3path_cancels = "s3://cdp-lcpr-process/em/feedback/cancels"

# .................................................................................................................
# def for email clicks

sql_clicks = """
select contact_id, launch_id, domain, email_sent_at, campaign_type, geo, platform, md5, is_mobile, is_anonymized, uid, ip, user_agent, section_id, link_id, category_id, is_img, campaign_id, message_id, event_time, customer_id, partitiontime, loaded_at, category_name,link_name, link_analysis_name,relative_link_id
from `sap-od-sapodlilac.emarsys_sapodlilac_837621947.email_clicks_837621947` 
WHERE contact_id is not null and date(event_time) = '{}'
""".format(today)


schema_clicks = StructType([
    StructField("contact_id", LongType(), True),
    StructField("launch_id", LongType(), True),
    StructField("domain", StringType(), True),
    StructField("email_sent_at", TimestampType(), True),
    StructField("campaign_type", StringType(), True),
    StructField(
        "geo",
        StructType([
            StructField("postal_code", StringType(), True),
            StructField("latitude", StringType(), True),
            StructField("longitude", StringType(), True),
            StructField("accuracy_radius", StringType(), True),
            StructField("continent_code", StringType(), True),
            StructField("country_iso_code", StringType(), True),
            StructField("continent_code", StringType(), True),
            StructField("city_name", StringType(), True),
            StructField("time_zone", StringType(), True),
        ]),
        True
        ),
    StructField("platform", StringType(), True),
    StructField("md5", StringType(), True),
    StructField("is_mobile", StringType(), True),
    StructField("is_anonymized", StringType(), True),
    StructField("uid", StringType(), True),
    StructField("ip", StringType(), True),
    StructField("user_agent", StringType(), True),
    StructField("section_id", StringType(), True),
    StructField("link_id", StringType(), True),
    StructField("category_id", StringType(), True) ,
    StructField("is_img", StringType(), True) ,
    StructField("campaign_id", StringType(), True) ,
    StructField("message_id", StringType(), True) ,
    StructField("event_time", TimestampType(), True),
    StructField("customer_id", StringType(), True),
    StructField("partitiontime", TimestampType(), True),
    StructField("loaded_at", TimestampType(), True),
    StructField("category_name", StringType(), True) ,
    StructField("link_name", StringType(), True) ,
    StructField("link_analysis_name", StringType(), True) ,
    StructField("relative_link_id", StringType(), True)
    ])


s3path_clicks = "s3://cdp-lcpr-process/em/feedback/clicks"

# .................................................................................................................
# def for email complaints
sql_complaints = """
select contact_id, launch_id,  domain, email_sent_at, campaign_type, campaign_id, message_id, event_time, customer_id, partitiontime, loaded_at
from `sap-od-sapodlilac.emarsys_sapodlilac_837621947.email_complaints_837621947` 
WHERE contact_id is not null and date(event_time) = '{}'
""".format(today)


schema_complaints = StructType([
    StructField("contact_id", LongType(), True),
    StructField("launch_id", LongType(), True),
    StructField("domain", StringType(), True),
    StructField("email_sent_at", TimestampType(), True),
    StructField("campaign_type", StringType(), True),
    StructField("campaign_id", LongType(), True) ,
    StructField("message_id", IntegerType(), True) ,
    StructField("event_time", TimestampType(), True),
    StructField("customer_id", LongType(), True),
    StructField("partitiontime", TimestampType(), True),
    StructField("loaded_at", TimestampType(), True)
    ])

s3path_complaints = "s3://cdp-lcpr-process/em/feedback/complaints"

# .................................................................................................................
# def for email unsubscribes
sql_unsubscribes = """
select contact_id, launch_id,  domain, email_sent_at, campaign_type, source, campaign_id, message_id, event_time, customer_id, loaded_at
from `sap-od-sapodlilac.emarsys_sapodlilac_837621947.email_unsubscribes_837621947` 
WHERE contact_id is not null and date(event_time) = '{}'
""".format(today)


schema_unsubscribes = StructType([
    StructField("contact_id", StringType(), True),
    StructField("launch_id", StringType(), True),
    StructField("domain", StringType(), True),
    StructField("email_sent_at", TimestampType(), True),
    StructField("campaign_type", StringType(), True),
    StructField("source", StringType(), True),
    StructField("campaign_id", StringType(), True) ,
    StructField("message_id", StringType(), True) ,
    StructField("event_time", TimestampType(), True),
    StructField("customer_id", StringType(), True),
    StructField("loaded_at", TimestampType(), True)
    ])

s3path_unsubscribes = "s3://cdp-lcpr-process/em/feedback/unsubscribes"
# .................................................................................................................
# realiza ingesta

ingest_feedback_email(mySql=sql_campaigns,mySchema=schema_campaigns,myPath=s3path_campaigns)
ingest_feedback_email(mySql=sql_sends,mySchema=schema_sends,myPath=s3path_sends)
ingest_feedback_email(mySql=sql_opens,mySchema=schema_opens,myPath=s3path_opens)
ingest_feedback_email(mySql=sql_bounces,mySchema=schema_bounces,myPath=s3path_bounces)
ingest_feedback_email(mySql=sql_cancels,mySchema=schema_cancels,myPath=s3path_cancels)
ingest_feedback_email(mySql=sql_clicks,mySchema=schema_clicks,myPath=s3path_clicks)
ingest_feedback_email(mySql=sql_complaints,mySchema=schema_complaints,myPath=s3path_complaints)
ingest_feedback_email(mySql=sql_unsubscribes,mySchema=schema_unsubscribes,myPath=s3path_unsubscribes)

# actualiza particiones
create_partition_v2('db_dev_cdp_project', 'feedback_email_campaigns', str(year)+str(month)+str(day))
create_partition_v2('db_dev_cdp_project', 'feedback_email_sends', str(year)+str(month)+str(day))
create_partition_v2('db_dev_cdp_project', 'feedback_email_opens', str(year)+str(month)+str(day))
create_partition_v2('db_dev_cdp_project', 'feedback_email_bounces', str(year)+str(month)+str(day))
create_partition_v2('db_dev_cdp_project', 'feedback_email_cancels', str(year)+str(month)+str(day))
create_partition_v2('db_dev_cdp_project', 'feedback_email_clicks', str(year)+str(month)+str(day))
create_partition_v2('db_dev_cdp_project', 'feedback_email_complaints', str(year)+str(month)+str(day))
create_partition_v2('db_dev_cdp_project', 'feedback_email_unsubscribes', str(year)+str(month)+str(day))


#genera datos de comunicación en el histórico
sql_comm = """
SELECT contact_id, 'email' as channel, campaign_id, c.name emarsys_name, event_time, launch_id
FROM ( SELECT  s.contact_id, s.campaign_id, s.event_time, s.launch_id
       FROM `sap-od-sapodlilac.emarsys_sapodlilac_837621947.email_sends_837621947` s 
       WHERE s.contact_id is not null and  date(event_time) = '{0}'  
       Union ALL
       SELECT c.contact_id, c.campaign_id, c.event_time, c.launch_id 
       FROM `sap-od-sapodlilac.emarsys_sapodlilac_837621947.email_cancels_837621947` c 
       WHERE c.contact_id is not null and  date(event_time) = '{0}'
       Union ALL
       SELECT b.contact_id, b.campaign_id, b.event_time, b.launch_id
       FROM `sap-od-sapodlilac.emarsys_sapodlilac_837621947.email_bounces_837621947` b 
       WHERE b.contact_id is not null and  date(event_time) = '{0}'
     )  Comm
LEFT JOIN (select t1.id, t1.name 
           from `sap-od-sapodlilac.emarsys_sapodlilac_837621947.email_campaigns_837621947` t1
           inner join (select t2.id, max(t2.event_time) as last_event_time
                       from `sap-od-sapodlilac.emarsys_sapodlilac_837621947.email_campaigns_837621947` t2
                       group by t2.id) m
           on t1.id=m.id and t1.event_time = m.last_event_time 
          ) c
on comm.campaign_id=c.id    
""".format(today)

schema_comm= StructType([
    StructField("contact_id", LongType(), True),
    StructField("channel", StringType(), True),
    StructField("campaign_id", LongType(), True) ,
    StructField("emarsys_name", StringType(), True) ,
    StructField("event_time", TimestampType(), True),
    StructField("launch_id", LongType(), True),
    ])


s3path_comm = "s3://cdp-lcpr-process/hist/communications_hist"

ingest_feedback_email(mySql=sql_comm,mySchema=schema_comm,myPath=s3path_comm)
'''
create_partition_v2('db_dev_cdp_project', 'communications_hist', str(year)+str(month)+str(day))
'''

job.commit()

