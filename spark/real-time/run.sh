#!/bin/bash
docker exec -it spark-master bash -c "cd home && cd real-time && /spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 /home/real-time/consumer/consumer.py"
