#!/bin/bash

docker exec mymysql bash -c "mysql -ustreamuser -pstream streamdb -e 'SELECT * from data_sink;'"