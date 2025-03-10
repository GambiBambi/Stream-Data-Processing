#!/bin/bash

export CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)

gsutil cp -r gs://$BUCKET_NAME/crimes-in-chicago_result.zip .

unzip crimes-in-chicago_result.zip
mv crimes-in-chicago_result data

kafka-topics.sh --create \
 --bootstrap-server ${CLUSTER_NAME}-w-1:9092 \
 --replication-factor 2 --partitions 3 --topic chicago-data

kafka-topics.sh --create \
 --bootstrap-server ${CLUSTER_NAME}-w-1:9092 \
 --replication-factor 2 --partitions 3 --topic count

 kafka-topics.sh --create \
 --bootstrap-server ${CLUSTER_NAME}-w-1:9092 \
 --replication-factor 2 --partitions 3 --topic json