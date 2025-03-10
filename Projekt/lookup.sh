#!/bin/bash

CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)

/usr/lib/kafka/bin/kafka-console-consumer.sh \
 --bootstrap-server ${CLUSTER_NAME}-w-1:9092 \
 --topic count