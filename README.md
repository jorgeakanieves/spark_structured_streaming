# Spark structured streaming app

## Project

Playing with analysis and collecting statistics from data creating time window dataframes, applying arithmetics functions and profiling over infinite data and pivoting data into some fields

## Overview

The main purpouse is building an end to end from a kafka topic, process data creating some naive analysis and windowing then saving
data to Elasticsearch index and creating a few analysis with Kibana

## Aggregations

- metrics sample (by window time):
- Count number of windows and ids
- Count unique/distinct ids (Duplicated data)
- Check min/ max/ unique/ avg/ sum data amount
- Etc…

## Data sample

{"id":"1","timeField":1451288431000,"amount":-37.97,"country":"SPAIN"}

## Requirements

- Message Broker	Confluent Kafka	0.10
- Stream Processing	Spark SQL Streaming	2.3.0
- Storage	Elasticsearch	6.2.2
- Visualization	Kibana	6.2.2

## Run

spark-submit  --jars elasticsearch-spark-20_2.11-6.2.2.jar,spark-sql-kafka-0-10_2.11-2.3.0.jar,kafka-clients-1.1.0.jar --class com.dev.jene.DataProfiling spark-structured-1.0.jar local

## Contributing

Feel free to become a contributor by pull request

## About

https://www.linkedin.com/in/jorgenieves/

100% open source and community-driven

## Resources

Apache Spark https://spark.apache.org/

Apache Kafka https://kafka.apache.org/

Elasticsearch https://www.elastic.co/








