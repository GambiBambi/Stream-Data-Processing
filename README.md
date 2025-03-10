# Crimes in Chicago - Kafka Streams Project

**Author: Julia Podsadna**

## Table of Contents
1. Project Configuration
2. Environment Setup
3. Data Stream Production, Processing, and Storage
4. Retrieving Processing Results
5. Resetting the Environment
6. Producer and Initialization Scripts
7. Real-Time Data Image Maintenance - Transformations
8. Real-Time Data Image Maintenance - Mode A Handling
9. Real-Time Data Image Maintenance - Mode C Handling
10. Data Stream Processing Program - Execution Scripts
11. Database Connection and Storage
12. Features of the Real-Time Data Image Storage

---

## Project Configuration

### Environment Setup

1. Start the cluster using the following command:
   ```sh
   gcloud dataproc clusters create ${CLUSTER_NAME} \
   --enable-component-gateway --region ${REGION} --subnet default \
   --master-machine-type n1-standard-4 --master-boot-disk-size 50 \
   --num-workers 2 --worker-machine-type n1-standard-2 --worker-boot-disk-size 50 \
   --image-version 2.1-debian11 --optional-components DOCKER,ZOOKEEPER \
   --project ${PROJECT_ID} --max-age=3h \
   --metadata "run-on-master=true" \
   --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/kafka/kafka.sh
   ```

2. Download the `crime-in-chicago-result.zip` dataset from:
   ```
   https://www.cs.put.poznan.pl/kjankiewicz/bigdata/stream_project/crimes-in-chicago_result.zip
   ```

3. Upload the ZIP file to your Google Cloud Storage bucket (do not place it in a subfolder).
4. Open 4 SSH terminals on the cluster master node.
5. Upload the project ZIP file using the "Upload" button in one of the terminals.
6. Unzip the project file, set execution permissions, and convert scripts to UNIX format:
   ```sh
   unzip project2.zip
   chmod +x *.sh
   sed -i 's/\r//' *.sh
   ```
7. Run the environment setup script and configure Kafka topics:
   ```sh
   export BUCKET_NAME=<your_bucket_name>
   ./create_environment_and_topics.sh
   ```
8. Create the database and establish a connection:
   ```sh
   ./create_db_and_db_connection.sh
   ```

---

## Data Stream Production, Processing, and Storage

9. In the first terminal, start monitoring the aggregated data topic:
   ```sh
   ./lookup.sh
   ```
10. In the second terminal, start fetching data from Kafka into the database:
    ```sh
    ./connect_db.sh
    ```
11. In the third terminal, start the Kafka producer:
    ```sh
    ./kafka_producer.sh
    ```
12. In the fourth terminal, start processing streams in Mode A:
    ```sh
    ./aggregator.sh
    ```
    - Alternatively, run Mode C processing:
      ```sh
      ./aggregatorC.sh
      ```
13. Once data appears in the first terminal, stop the processes using `CTRL+C`.

---

## Retrieving Processing Results

14. To retrieve all data from the database:
    ```sh
    ./all_data.sh
    ```
15. To retrieve specific data (crime category, district, and month):
    ```sh
    ./read_db.sh "THEFT" 14 "2001-04"
    ```

---

## Resetting the Environment

16. To reset the environment and database:
    ```sh
    ./reset.sh
    ```

---

## Producer and Initialization Scripts

- **Environment Setup Script (`create_environment_and_topics.sh`)**: Downloads data from the bucket, unzips it, renames directories, and creates Kafka topics.
- **Kafka Producer Script (`kafka_producer.sh`)**: Sends data to Kafka topics using a `.jar` producer.
- **Reset Script (`reset.sh`)**: Resets the entire environment and database.

---

## Real-Time Data Image Maintenance - Transformations

### Data Transformation Steps:
1. **Mapping Input Data**
   ```java
   .map((key, value) -> {
       String newValue = String.format("IUCR: %s, Category: %s, District: %f, Date: %s, Arrest: %b, Domestic: %b, IndexCode: %s",
           value.getIUCR(), value.getCategory(), value.getDistrict(), value.getMonthYear(),
           value.getArrest(), value.getDomestic(), value.getIndexCode());
       return KeyValue.pair(value.getCategory() + "-" + value.getDistrict(), newValue);
   })
   ```
2. **Grouping by Key**
   ```java
   .groupByKey()
   ```
3. **Defining Time Windows**
   ```java
   .windowedBy(TimeWindows.of(Duration.ofDays(30)).grace(gracePeriod))
   ```
4. **Aggregation**
   ```java
   .aggregate(
       () -> "Initial State",
       (aggKey, newValue, aggregate) -> {
           return updateAggregate(aggregate, newValue);
       },
       Materialized.with(stringSerde, stringSerde)
   )
   ```

---

## Real-Time Data Image Maintenance - Mode A Handling

- Emits partial aggregation results as data arrives.
- Results are finalized after the time window closes.

## Real-Time Data Image Maintenance - Mode C Handling

- Suppresses intermediate results until the window closes:
  ```java
  .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));
  ```

---

## Data Stream Processing Program - Execution Scripts

- **Mode A Processing (`aggregator.sh`)**: Aggregates crime data by category and district.
- **Mode C Processing (`aggregatorC.sh`)**: Similar to Mode A but delays results until the window closes.

---

## Database Connection and Storage

### Database Setup Scripts
- **Create Database and Connection (`create_db_and_db_connection.sh`)**
- **Start Database Listener (`connect_db.sh`)**

### Features of MySQL Storage
- Mature and widely adopted technology
- JDBC Connector support
- Easy deployment via Docker
- Seamless integration with Apache Kafka

---

## Summary
This project utilizes Kafka Streams to process real-time crime data from Chicago. The system aggregates and stores crime reports, supporting both immediate and delayed result processing modes. The project integrates Apache Kafka with MySQL, ensuring efficient real-time data analysis.

