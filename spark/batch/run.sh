#!/bin/bash

docker exec -it spark-master bash -c "cd home && cd batch && /spark/bin/spark-submit /home/batch/transformations.py"
