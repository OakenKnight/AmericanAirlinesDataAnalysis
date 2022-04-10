#!/bin/bash

docker exec -it spark-master bash -c "cd home && cd batch && /spark/bin/spark-submit /home/batch/transformations.py"
docker exec -it spark-master bash -c "cd home && cd batch && /spark/bin/spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 /home/batch/currated.py"
