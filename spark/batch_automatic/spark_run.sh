#!/bin/bash

/spark/bin/spark-submit /home/batch/transformations.py &

/bin/bash /master.sh
