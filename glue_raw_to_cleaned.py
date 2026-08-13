import sys
import time
import boto3
from datetime import datetime, timezone
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, trim, upper, when, lit, current_timestamp,
    to_timestamp, to_date, year, concat, lpad
)

# --- Initialise Glue Job ---
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

start_time = time.time()

# --- STEP 1: Read batch source from Glue Data Catalog (raw zone) ---
batch_source = glueContext.create_dynamic_frame.from_catalog(
    database='ade_ae1_datalake_db',
    table_name='raw_batch_la_collisions'
)
df_batch = batch_source.toDF()
batch_read = df_batch.count()
print(f'Batch records read from raw zone: {batch_read}')
df_batch.printSchema()

# --- STEP 2: Batch - build event timestamp ---
# 'date occurred' is MM/dd/yyyy; 'time occurred' was inferred as bigint,
# so 0130 arrives as 130 and needs padding back to four digits
df_batch = df_batch.withColumn('event_timestamp', to_timestamp(
    concat(col('date occurred'), lit(' '),
           lpad(col('time occurred').cast('string'), 4, '0')),
    'MM/dd/yyyy HHmm'))

# --- STEP 3: Batch - handle missing and sentinel values ---
# Age 99 is a sentinel for unknown, not a real age
df_batch = df_batch.withColumn('victim age',
    when(col('victim age') == 99, lit(None)).otherwise(col('victim age')))

# Fill missing dimension values
df_batch = df_batch.fillna({'area name': 'UNKNOWN',
                            'premise description': 'UNKNOWN'})

# --- STEP 4: Batch - data quality ---
# Remove rows with no key or an unparseable date
df_batch_valid = df_batch.filter(
    col('dr number').isNotNull() & col('event_timestamp').isNotNull())
batch_valid = df_batch_valid.count()
print(f'Batch records after validation: {batch_valid}')

# --- STEP 5: Batch - map to unified schema ---
# 'crime code description' dropped: constant across all rows
# 'mo codes' dropped: space-separated multi-value, not atomic
df_batch_clean = df_batch_valid.select(
    col('dr number').cast('string').alias('event_id'),
    col('event_timestamp'),
    trim(col('area name')).alias('region'),
    trim(col('premise description')).alias('category'),
    col('victim age').cast('double').alias('metric_value'),
    lit('la_collisions').alias('source'))

# --- STEP 6: Read streaming source from Glue Data Catalog (raw zone) ---
stream_source = glueContext.create_dynamic_frame.from_catalog(
    database='ade_ae1_datalake_db',
    table_name='raw_streaming_streaming'
)
df_stream = stream_source.toDF()
stream_read = df_stream.count()
print(f'Streaming records read from raw zone: {stream_read}')

# --- STEP 7: Streaming - deduplicate (at-least-once delivery handling) ---
df_stream = df_stream.dropDuplicates(['event_id'])
stream_dedup = df_stream.count()
print(f'Streaming records after dedup: {stream_dedup}')

# --- STEP 8: Streaming - parse timestamp and fill missing values ---
df_stream = df_stream.withColumn('event_timestamp',
    to_timestamp(col('event_timestamp'), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
df_stream = df_stream.fillna({'region': 'UNKNOWN', 'category': 'UNKNOWN'})

df_stream_valid = df_stream.filter(
    col('event_id').isNotNull() & col('event_timestamp').isNotNull())
stream_valid = df_stream_valid.count()
print(f'Streaming records after validation: {stream_valid}')

# --- STEP 9: Streaming - map to unified schema ---
df_stream_clean = df_stream_valid.select(
    col('event_id'),
    col('event_timestamp'),
    trim(col('region')).alias('region'),
    trim(col('category')).alias('category'),
    col('metric_value').cast('double').alias('metric_value'),
    lit('crossref').alias('source'))

# --- STEP 10: Merge both sources into the unified structure ---
# Union on a conformed date dimension with a 'source' discriminator;
# the two datasets share no key, so a join would be meaningless
df_unified = df_batch_clean.unionByName(df_stream_clean)

# --- STEP 11: Add derived and metadata columns ---
df_unified = df_unified.withColumn('event_date', to_date(col('event_timestamp')))
df_unified = df_unified.withColumn('year', year(col('event_timestamp')))
df_unified = df_unified.withColumn('processing_timestamp', current_timestamp())

unified_count = df_unified.count()
print(f'Unified records: {unified_count}')
df_unified.printSchema()
df_unified.groupBy('source').count().show()

# --- STEP 12: Write to cleaned zone as partitioned Parquet ---
cleaned_path = 's3://ade-ae1-datalake-jd/cleaned/unified_events/'
df_unified.write.mode('overwrite').partitionBy('year').parquet(cleaned_path)
print(f'Unified records written to cleaned zone: {unified_count}')

# --- STEP 13: Log run metadata to DynamoDB ---
duration_ms = int((time.time() - start_time) * 1000)
run_id = f"glue-etl-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}"

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('ade-ae1-pipeline-metadata')
table.put_item(Item={
    'run_id': run_id,
    'timestamp': datetime.now(timezone.utc).isoformat(),
    'pipeline_stage': 'glue_etl_raw_to_cleaned',
    'source': 'batch+streaming',
    'batch_records_read': batch_read,
    'batch_records_valid': batch_valid,
    'stream_records_read': stream_read,
    'stream_duplicates_removed': stream_read - stream_dedup,
    'records_written': unified_count,
    'duration_ms': duration_ms,
    'status': 'SUCCESS'})

print(f'Metadata logged: {run_id}. Duration: {duration_ms} ms')

job.commit()
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()