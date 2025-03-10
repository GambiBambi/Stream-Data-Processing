#!/bin/bash


if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <category> <district> <date>"
    exit 1
fi
  

CATEGORY=$1
DISTRICT=$2
DATE=$3


RESULT=$(docker exec mymysql bash -c "mysql -ustreamuser -pstream streamdb -e \"SELECT * FROM data_sink WHERE category='$CATEGORY' AND district='$DISTRICT' AND date='$DATE' ORDER BY id DESC LIMIT 1;\"")


if [ -z "$RESULT" ]; then
    echo "No data found for category $CATEGORY and district $DISTRICT on date $DATE"
    exit 1
fi


echo "$RESULT" | while IFS=$'\t' read -r id category district date all arrest domestic fbi; do
    if [ "$id" != "id" ]; then
        echo "Category: $category"
        echo "District: $district"
        echo "Date: $date"
        echo "Number of all crimes: $all"
        echo "Number of crimes that ended up with an arrest: $arrest"
        echo "Number of domestic crimes: $domestic"
        echo "Number of crimes reported by FBI: $fbi"
    fi
done