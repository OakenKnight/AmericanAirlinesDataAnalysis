#!/usr/bin/python

import os
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import explode
from pyspark.sql.functions import create_map
from pyspark.sql.functions import col
from pyspark.sql.functions import expr
from pyspark.sql.types import *

print('=========================== Transformations started ===========================')
Hdf_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

spark = SparkSession.builder.appName("airplane delays transformations").getOrCreate()

dfDelaysTotal2013FromCSV = spark.read.option("multiline", "true").option("sep", ",").option("header", "true") \
    .option("inferSchema", "true") \
    .csv(Hdf_NAMENODE + "/data/batch-2013.csv")

dfDelaysTotal2014FromCSV = spark.read.option("multiline", "true").option("sep", ",").option("header", "true") \
    .option("inferSchema", "true") \
    .csv(Hdf_NAMENODE + "/data/batch-2014.csv")

dfDelaysTotal2015FromCSV = spark.read.option("multiline", "true").option("sep", ",").option("header", "true") \
    .option("inferSchema", "true") \
    .csv(Hdf_NAMENODE + "/data/batch-2015.csv")

dfDelaysTotal2016FromCSV = spark.read.option("multiline", "true").option("sep", ",").option("header", "true") \
    .option("inferSchema", "true") \
    .csv(Hdf_NAMENODE + "/data/batch-2016.csv")

dfDelaysTotal2017FromCSV = spark.read.option("multiline", "true").option("sep", ",").option("header", "true") \
    .option("inferSchema", "true") \
    .csv(Hdf_NAMENODE + "/data/batch-2017.csv")

print("===================== DELETING NOT USED COLUMNS =====================")

dfDelaysTotal2013 = dfDelaysTotal2013FromCSV.drop("OP_CARRIER_FL_NUM","CANCELLATION_CODE", "TAXI_OUT", "WHEELS_OFF", "WHEELS_ON"
                                                  "DIVERTED", "Unnamed: 27\r", "TAXI_IN")
dfDelaysTotal2014 = dfDelaysTotal2014FromCSV.drop("OP_CARRIER_FL_NUM","CANCELLATION_CODE", "TAXI_OUT", "WHEELS_OFF", "WHEELS_ON"
                                                  "DIVERTED", "Unnamed: 27\r", "TAXI_IN")
dfDelaysTotal2015 = dfDelaysTotal2015FromCSV.drop("OP_CARRIER_FL_NUM","CANCELLATION_CODE", "TAXI_OUT", "WHEELS_OFF", "WHEELS_ON"
                                                  "DIVERTED", "Unnamed: 27\r", "TAXI_IN")
dfDelaysTotal2016 = dfDelaysTotal2016FromCSV.drop("OP_CARRIER_FL_NUM","CANCELLATION_CODE", "TAXI_OUT", "WHEELS_OFF", "WHEELS_ON"
                                                  "DIVERTED", "Unnamed: 27\r", "TAXI_IN")
dfDelaysTotal2017 = dfDelaysTotal2017FromCSV.drop("OP_CARRIER_FL_NUM","CANCELLATION_CODE", "TAXI_OUT", "WHEELS_OFF", "WHEELS_ON"
                                                  "DIVERTED", "Unnamed: 27\r", "TAXI_IN")

print("===================== DONE DELETING COLUMNS =====================")

print("===================== DOING UNION IN ONE BIG DATAFRAME=====================")
dfDelaysTotalYrs1 = dfDelaysTotal2013.union(dfDelaysTotal2014)
dfDelaysTotalYrs2 = dfDelaysTotalYrs1.union(dfDelaysTotal2015)
dfDelaysTotalYrs3 = dfDelaysTotalYrs2.union(dfDelaysTotal2016)
dfDelaysTotalYrs=dfDelaysTotalYrs3.union(dfDelaysTotal2017)
print("===================== DONE =====================")

customAggregatedSchemaG = StructType() \
    .add("OP_CARRIER", StringType()) \
    .add("NUM_OF_FLIGHTS", IntegerType()) \
    .add("NUM_OF_DELAYED_FLIGHTS", IntegerType()) \
    .add("TOTAL_DELAY_ALL_TIME", IntegerType()) \
    .add("TOTAL_CANCELLED", IntegerType()) \

    
# FL_DATE   1
# OP_CARRIER 1
# ORIGIN 1
# DEST 1
# CRS_DEP_TIME 1
# DEP_TIME 1
# DEP_DELAY OBAVEZNO
# TAXI_OUT 0
# WHEELS_OFF 0
# WHEELS_ON 0
# TAXI_IN 0
# CRS_ARR_TIME 1
# ARR_TIME 1
# ARR_DELAY OBAVEZNO
# CANCELLED 1
# CRS_ELAPSED_TIME 
# ACTUAL_ELAPSED_TIME
# AIR_TIME 
# DISTANCE 1
# CARRIER_DELAY nekako merge
# WEATHER_DELAY nekako merge
# NAS_DELAY nekako merge
# SECURITY_DELAY nekako merge
# LATE_AIRCRAFT_DELAY nekako merge


# while True:
#     try:
#         dfDelaysTotalYrs.write.option("header", "true").csv(Hdf_NAMENODE + "/transformation_layer/totalDelays2013-2017", mode="ignore")
#         print("\n\n<<<<<<<<<< SPARK WROTE DELAY INFO FOR YEARS 2013 - 2017 TO Hdf >>>>>>>>>>>>>>>\n\n")
#         break
#     except:
#         print("<<<<<<<<<<<<<< Failure! Trying again... >>>>>>>>>>>>")
#         time.sleep(5)

dfDelaysTotalYrs.unpersist()


dfDelaysTotalYrs.show(truncate=False)
# dfDelays2018.show(10)
