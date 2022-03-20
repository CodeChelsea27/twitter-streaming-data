import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import explode, lit
from datetime import date, timedelta
import boto3

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


def read_s3_bucket():
    s3_path = f"s3://twitter-staging-data-bucket/{file_key}"
    input_dynamic_frame = glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={"paths": [s3_path], "recurse": True}
    )
    input_dataframe = input_dynamic_frame.toDF()
    return input_dataframe


def transform_twitter_data(df):
    df_transform_raw = df.select(df.data, explode(df.matching_rules))
    df_transform_with_cols = df_transform_raw.select(
        "data.id", "data.text", "col.tag")
    return df_transform_with_cols


def get_date_params():
    today = date.today()
    yesterday = today - timedelta(days=1)
    year = str(yesterday.year).strip(' ')
    month = yesterday.month
    if month < 10:
        month = '0' + str(month)
    else:
        month = str(month)
    day = yesterday.day
    if day < 10:
        day = '0' + str(day)
    else:
        day = str(day)
    month = month.strip(' ')
    day = day.strip(' ')
    return year, month, day


def flush_processing_if_exists(bucket_name):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    bucket.objects.filter(Prefix=file_key).delete()


def write_parquet_to_s3(df):
    bucket_name = 'twitter-processed-data-bucket'
    flush_processing_if_exists(bucket_name)
    df_partitioned = df.withColumn(
        'year',
        lit(year)).withColumn(
        'month',
        lit(month)).withColumn(
            'day',
        lit(day))
    df_partitioned.write.mode('append').parquet(
        f"s3a://{bucket_name}/{file_key}")


year, month, day = get_date_params()
file_key = f"{year}/{month}/{day}/"
s3_input_df = read_s3_bucket()
s3_transformed_df = transform_twitter_data(s3_input_df)
write_parquet_to_s3(s3_transformed_df)
