# Project 5: Data Pipelines with Airflow

## Introduction

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.  

The objective of this project is to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. Data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.  

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Project Datasets
The input datasets are reside in S3. Here are the S3 links for each:

Song data: ```s3://udacity-dend/song_data```  
Log data: ```s3://udacity-dend/log_data```

## Project Instructions
A [project template](https://s3.amazonaws.com/video.udacity-data.com/topher/2019/February/5c6058dc_project-template/project-template.zip) has been provided to complete the data pipelines. The detailed project instructions can be found [here](Project_Instructions.md).


`./airflow` folder consists of all the files needed to create the data pipelines using Apache Airflow. The complete DAG of the data pipelines looks like this:

![](./dag.png)
