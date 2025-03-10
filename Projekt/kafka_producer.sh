#!/bin/bash

export CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)

java -cp /usr/lib/kafka/libs/*:KafkaProducer.jar com.example.bigdata.TestProducer data 15 chicago-data 0 ${CLUSTER_NAME}-w-0:9092