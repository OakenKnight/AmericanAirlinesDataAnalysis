#!/bin/bash

# while ! jps | grep -q NameNode
# do
#     echo ">>>>>>>> NAMENODE NOT UP <<<<<<<<<"
#     sleep 3
# done
# echo ">>>>>>>>>> NAMENODE UP <<<<<<<<<<"


# while ! hdfs dfs -test -d /batch
# do
#     echo ">>>>>>>>>>>DIRECTORY DOES NOT EXIST<<<<<<<<<<<<"
#     sleep 3
#     hdfs dfs -put /home/data /batch
# done
# echo ">>>>>>>>>>>DIRECTORY IS ON HDFS<<<<<<<<<<<<<<<<<"


docker cp ../data/ namenode:/home

docker cp . namenode:/home

docker exec -it namenode bash -c "hdfs dfsadmin -safemode leave"
docker exec -it namenode bash -c "hdfs dfs -rm -r -f /results*"
docker exec -it namenode bash -c "hdfs dfs -rm -r -f /data*"


docker exec -it namenode bash -c "hdfs dfs -mkdir /results"
docker exec -it namenode bash -c "hdfs dfs -mkdir /data"

docker exec -it namenode bash -c "hdfs dfs -put /home/data/* /data"
