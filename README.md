# twitter-streaming-data

Problem Statement : Extract and Analyse live Twitter streams

Files Attached - 

1. Twitter Data Pipeline POC : Includes below details
    - Implementation Details
    - Design 
    - Output Snips
    - POC Consideration and Enhancement Scope

2. requirements.txt - modules used in lambda layer

3. data-modules-layer.zip - zipped file containing layer structure. Can be imported directed into AWS and used with lambdas

4. lambda function scripts (python) - 
    - get_twitter_data.py : contains logic to call Twitter streaming data API using Filtered Streams.
                            For this POC we are using #covid and #ukraine tweets to filter stream
    - invoke_glue_job.py : triggered by CRON job at 1:00 am CMT everyday, calls data processing glue job

5. glue job scripts (pyspark) - 
    - transform_twitter_data_glue_job.py : glue job script for running batch processing on the streamed dumps

6. athena-twiter-streaming-notebook - Sample notebook for connecting to the Data Catalog

![image](https://user-images.githubusercontent.com/55575951/159493410-cdbb32a0-15af-4d77-8237-f09ef2569ecf.png)