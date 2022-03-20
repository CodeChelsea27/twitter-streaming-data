import json
import boto3


def lambda_handler(event, context):
    call_glue_job()


def call_glue_job():
    client = boto3.client('glue')
    response = client.start_job_run(
        JobName='twitter-data-processing-job')
