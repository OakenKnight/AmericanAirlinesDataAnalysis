#!/usr/bin/python

import os
import time
from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as F

from pyspark.sql.types import *

print('\n\n=========================== Transformations started ===========================\n\n')

Hdf_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

spark = SparkSession.builder.appName("airplane delays transformations").getOrCreate()

# BEGINNING OF TRANSFORMATION ZONE
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



print("\n\n===================== DOING UNION IN ONE BIG DATAFRAME =====================\n\n")
dfDelaysTotalYrs1 = dfDelaysTotal2013.union(dfDelaysTotal2014)
dfDelaysTotalYrs2 = dfDelaysTotalYrs1.union(dfDelaysTotal2015)
dfDelaysTotalYrs3 = dfDelaysTotalYrs2.union(dfDelaysTotal2016)
dfDelaysTotalYrs=dfDelaysTotalYrs3.union(dfDelaysTotal2017)

dfDelaysTotalYrs = dfDelaysTotalYrs.withColumn('DEP_DELAY',F.when(F.col("DEP_DELAY") > 0,F.col("DEP_DELAY")).otherwise(0))
dfDelaysTotalYrs = dfDelaysTotalYrs.withColumn('ARR_DELAY',F.when(F.col("ARR_DELAY") > 0,F.col("ARR_DELAY")).otherwise(0))


print("\n\n===================== DOING TRANSFORMATION FOR AIRPORTS DATAFRAME =====================\n\n")

dfAirportsAndDelays = dfDelaysTotalYrs.select(F.col("ORIGIN"), F.col("DEST"), F.col("DEP_DELAY"), F.col('ARR_DELAY'))
dfAirportsAndDelays = dfAirportsAndDelays.withColumn('DEP_DELAY',F.when(F.col("DEP_DELAY") > 0,F.col("DEP_DELAY")).otherwise(0))
dfAirportsAndDelays = dfAirportsAndDelays.withColumn('ARR_DELAY',F.when(F.col("ARR_DELAY") > 0,F.col("ARR_DELAY")).otherwise(0))

dfAirportsAndDelays.show()

print("\n\n===================== DONE WITH TRANSFORMATIONS =====================\n\n")


while True:
    try:
        dfDelaysTotalYrs.write.option("header", "true").csv(Hdf_NAMENODE + "/transformation_layer/dfDelaysTotalYrs", mode="ignore")
        print("\n\n<<<<<<<<<< SPARK WROTE DELAY FOR TOTAL YEARS INFO TO HDFS >>>>>>>>>>>>>>>\n\n")
        break
    except:
        print("<<<<<<<<<<<<<< Failure! Trying again... >>>>>>>>>>>>")
        time.sleep(5)

while True:
    try:
        dfAirportsAndDelays.write.option("header", "true").csv(Hdf_NAMENODE + "/transformation_layer/dfAirportsAndDelays", mode="ignore")
        print("\n\n<<<<<<<<<< SPARK WROTE DELAY FOR TOTAL YEARS INFO TO HDFS >>>>>>>>>>>>>>>\n\n")
        break
    except:
        print("<<<<<<<<<<<<<< Failure! Trying again... >>>>>>>>>>>>")
        time.sleep(5)



# END OF TRANSFORMATION ZONE




# DONE
# 1.	Na koji način se rangiraju aerodromi kada je u pitanju kašnjenje pri polasku? 
# Sortirati aerodrome prema DEP_DELAY
# 2.	Na koji način se rangiraju aerodromi kada je u pitanju kašnjenje pri dolasku? 
# Sortirati aerodrome prema ARR_DELAY
# 3.	Na koji način se rangiraju aviokompanije kada je u pitanju kašnjenje? 
# Nisam siguran da li treba sortirati aviokompanije samo prema CARRIER_DELAY ili prema zbiru CARRIER_DELAY, WEATHER_DELAY, NAS_DELAY, SECURITY_DELAY, LATE_AIRCRAFT_DELAY
# 10.	Koja je avio kompanija sa najviše odustanaka u tromesečnom periodu? 
# Vidi CANCELLED pa po kompanijama u tromesecnom periodu
# 7.	Koji je najgori period u toku dana, sa pragom od 3h? 
# Isto ta dva delaya samo na nivou dana, opet pazi na praznike, nisu karakteristicni
# 8.	Na koji način se rangiraju aviokompanije kada je u pitanju razlika između procenjenog vremena i vremena koje je stvarno proteklo od polaska do dolaska? 
# Razlika CRS ELAPSED TIME I ACTUAL ELAPSED TIME

# NO_DATA
# 5.	Koji su najkritičniji periodi u godini za letenje?  Cilj je pokušati utvrditi korelaciju sa nekim praznicima. 
# Ovo vrv moze se gledati i prema aerodromima I prema kompanijama. Mozda najbolje gledati spram ARR DELAY I DEP DELAY, za celu godinu pa izvuci dane. Vrv ce to biti thanksgiving. 4. Jul, bozic itd., a mozda i leto u nekim periodima
# 6.	Koji je najkritičniji dan u nedelji za letenje? 
# Isto preko delay i ARR I DEP, samo na nedeljnom nivou. Mozda ce odskakati ove praznicne nedelje, pa mozda treba njih izbaciti.




# HAS TO COMBINE ANOTHER TABLE, MAYBE TOO MUCH FOR FIRST PHASE
#druga tabela
# 11.	Koje su države koje su najgore po pitanju kašnjenja?
# Pogledaj DELAY i ARR i DEP i vidi po stejtovima

