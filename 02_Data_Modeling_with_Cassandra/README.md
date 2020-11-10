# Project: Data Modeling with Cassandra

## Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app. The objective of this project is to create a Apache Cassandra database to optimize queries on song play analysis.

## Project Datasets

There is only one dataset for this project: `event_data`. The directory of CSV files partitioned by date. Here are examples of filepaths to two files in the dataset:

```
event_data/2018-11-08-events.csv
event_data/2018-11-09-events.csv
```

## Project Steps

The detailed steps to build the ETL pipeline for pre-processing the datasets and modeling the Apache Cassandra database can be found in this [notebook](./Project_1B.ipynb).
