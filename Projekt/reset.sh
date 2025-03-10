#!/bin/bash
export CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)

kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic __consumer_offsets
kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic chicago-data
kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic json
kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic count


kafka-topics.sh --create \
 --bootstrap-server ${CLUSTER_NAME}-w-1:9092 \
 --replication-factor 2 --partitions 3 --topic chicago-data

kafka-topics.sh --create \
 --bootstrap-server ${CLUSTER_NAME}-w-1:9092 \
 --replication-factor 2 --partitions 3 --topic count

 kafka-topics.sh --create \
 --bootstrap-server ${CLUSTER_NAME}-w-1:9092 \
 --replication-factor 2 --partitions 3 --topic json

#------------------------------------------------------------------------------

docker exec mymysql bash -c "mysql -ustreamuser -pstream streamdb -e 'DROP TABLE data_sink;'"

rm connect-jdbc-sink.properties
rm connect-standalone.properties
rm kafka-connect-jdbc-10.7.0.jar
rm mysql-connector-j-8.0.33.jar
sudo rm -rf /tmp/datadir
sudo rm -rf /usr/lib/kafka/plugin

docker stop mymysql
docker rm mymysql
docker rmi mysql:debian


mkdir -p /tmp/datadir

docker run --name mymysql -v /tmp/datadir:/var/lib/mysql -p 6033:3306 \
  -e MYSQL_ROOT_PASSWORD=my-secret-pw -d mysql:debian

sleep 20

docker exec mymysql bash -c "mysql -uroot -pmy-secret-pw -e '
    CREATE USER \"streamuser\"@\"%\" IDENTIFIED BY \"stream\";
    CREATE DATABASE IF NOT EXISTS streamdb CHARACTER SET utf8;
    GRANT ALL ON streamdb.* TO \"streamuser\"@\"%\";
'"

docker exec mymysql bash -c "mysql -ustreamuser -pstream streamdb -e '
    CREATE TABLE IF NOT EXISTS data_sink (
        id int NOT NULL AUTO_INCREMENT,
        category VARCHAR(200),
        district FLOAT,
        date VARCHAR(200),
        all_count INTEGER,
        arrest INTEGER,
        domestic INTEGER,
        fbi INTEGER,
        PRIMARY KEY (id)
    );
'"

wget https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar

sudo cp mysql-connector-j-8.0.33.jar /usr/lib/kafka/libs

wget https://packages.confluent.io/maven/io/confluent/kafka-connect-jdbc/10.7.0/kafka-connect-jdbc-10.7.0.jar

sudo mkdir /usr/lib/kafka/plugin
sudo cp kafka-connect-jdbc-10.7.0.jar /usr/lib/kafka/plugin

configlines1="
plugin.path=/usr/lib/kafka/plugin
bootstrap.servers=localhost:9092
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=false
value.converter.schemas.enable=true
offset.storage.file.filename=/tmp/connect.offsets
offset.flush.interval.ms=10000
"
echo "$configlines1" >> connect-standalone.properties

configlines2="
connection.url=jdbc:mysql://localhost:6033/streamdb
connection.user=streamuser
connection.password=stream
tasks.max=1
name=kafka-to-mysql-task
connector.class=io.confluent.connect.jdbc.JdbcSinkConnector
topics=json
table.name.format=data_sink
delete.enabled=false
pk.mode=none
pk.fields=id
"

echo "$configlines2" >> connect-jdbc-sink.properties

sudo cp /usr/lib/kafka/config/tools-log4j.properties /usr/lib/kafka/config/connect-log4j.properties

echo "log4j.logger.org.reflections=ERROR" | sudo tee -a /usr/lib/kafka/config/connect-log4j.properties


