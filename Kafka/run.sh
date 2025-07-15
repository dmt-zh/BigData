#!/usr/bin/env bash

###############################################################################

LOG_DIR="${PWD}"/logs
mkdir "${LOG_DIR}"

###############################################################################

docker compose up -d

###############################################################################

docker logs kafka-server --tail 1 | grep -q "Kafka Server started" &> /dev/null
for i in {1..5}; do
    if docker logs kafka-server --tail 1 2>1 | grep -q "Kafka Server started" ; then
        echo "Kafka Server started"
        break
    else
        echo "Checking Kafka Server logs..."
        [ $i -lt 5 ] && sleep 10s  # Ждем только если это не последняя попытка
        if [ $i == 5 ]; then
            echo "Something went wrong, Kafka server not started. Check container logs."
            exit 1
        fi
    fi
done

###############################################################################

echo "Starting streaming from S3 to Kafka server"
nohup env/bin/python3.12 producer.py &> "${LOG_DIR}"/producer.log &

###############################################################################

echo "Going to sleep 1m sec waiting for data flud"
sleep 1m

###############################################################################

echo "Starting Srark Streaming worker"
nohup env/bin/python3.12 spark_streaming.py &> ~/logs/consumer.log &
