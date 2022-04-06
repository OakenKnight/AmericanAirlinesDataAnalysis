#!/usr/bin/python

import os
from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as F

from pyspark.sql.types import *

print('=========================== Transformations started ===========================')
Hdf_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

spark = SparkSession.builder.appName("airplane delays transformations").getOrCreate()

dfDelaysTotal2013FromCSV = spark.read.option("multiline", "true").option("sep", ",").option("header", "true") \
    .option("inferSchema", "true") \
    .csv(Hdf_NAMENODE + "/data/batch-2013.csv")

# dfDelaysTotal2014FromCSV = spark.read.option("multiline", "true").option("sep", ",").option("header", "true") \
#     .option("inferSchema", "true") \
#     .csv(Hdf_NAMENODE + "/data/batch-2014.csv")

# dfDelaysTotal2015FromCSV = spark.read.option("multiline", "true").option("sep", ",").option("header", "true") \
#     .option("inferSchema", "true") \
#     .csv(Hdf_NAMENODE + "/data/batch-2015.csv")

# dfDelaysTotal2016FromCSV = spark.read.option("multiline", "true").option("sep", ",").option("header", "true") \
#     .option("inferSchema", "true") \
#     .csv(Hdf_NAMENODE + "/data/batch-2016.csv")

# dfDelaysTotal2017FromCSV = spark.read.option("multiline", "true").option("sep", ",").option("header", "true") \
#     .option("inferSchema", "true") \
#     .csv(Hdf_NAMENODE + "/data/batch-2017.csv")

print("===================== DELETING NOT USED COLUMNS =====================")

dfDelaysTotal2013 = dfDelaysTotal2013FromCSV.drop("OP_CARRIER_FL_NUM","CANCELLATION_CODE", "TAXI_OUT", "WHEELS_OFF", "WHEELS_ON"
                                                  "DIVERTED", "Unnamed: 27\r", "TAXI_IN")
# dfDelaysTotal2014 = dfDelaysTotal2014FromCSV.drop("OP_CARRIER_FL_NUM","CANCELLATION_CODE", "TAXI_OUT", "WHEELS_OFF", "WHEELS_ON"
#                                                   "DIVERTED", "Unnamed: 27\r", "TAXI_IN")
# # dfDelaysTotal2015 = dfDelaysTotal2015FromCSV.drop("OP_CARRIER_FL_NUM","CANCELLATION_CODE", "TAXI_OUT", "WHEELS_OFF", "WHEELS_ON"
#                                                   "DIVERTED", "Unnamed: 27\r", "TAXI_IN")
# dfDelaysTotal2016 = dfDelaysTotal2016FromCSV.drop("OP_CARRIER_FL_NUM","CANCELLATION_CODE", "TAXI_OUT", "WHEELS_OFF", "WHEELS_ON"
#                                                   "DIVERTED", "Unnamed: 27\r", "TAXI_IN")
# dfDelaysTotal2017 = dfDelaysTotal2017FromCSV.drop("OP_CARRIER_FL_NUM","CANCELLATION_CODE", "TAXI_OUT", "WHEELS_OFF", "WHEELS_ON"
#                                                   "DIVERTED", "Unnamed: 27\r", "TAXI_IN")
print("===================== DONE DELETING COLUMNS =====================")

print("===================== DOING UNION IN ONE BIG DATAFRAME=====================")
# dfDelaysTotalYrs1 = dfDelaysTotal2013.union(dfDelaysTotal2014)
# dfDelaysTotalYrs2 = dfDelaysTotalYrs1.union(dfDelaysTotal2015)
# dfDelaysTotalYrs3 = dfDelaysTotalYrs2.union(dfDelaysTotal2016)
# dfDelaysTotalYrs=dfDelaysTotalYrs3.union(dfDelaysTotal2017)
print("===================== DONE =====================")


# 2018-01-01	UA	488	MFE	IAH	1726							1844			1	B	0	78			316

# 2013-01-01,EV,4486,IAH,SAV,1933,,,,,,,2240,,,1.0,C,0.0,127.0,,,851.0,,,,,,

# 2013-01-11,UA,353,LAX,EWR,1532,,,,,,,2355,,,1.0,A,0.0,323.0,,,2454.0,,,,,,
# FL_DATE,OP_CARRIER,OP_CARRIER_FL_NUM,ORIGIN,DEST,CRS_DEP_TIME,DEP_TIME,DEP_DELAY,TAXI_OUT,WHEELS_OFF,WHEELS_ON,TAXI_IN,CRS_ARR_TIME,ARR_TIME,ARR_DELAY,CANCELLED,CANCELLATION_CODE,DIVERTED,CRS_ELAPSED_TIME,ACTUAL_ELAPSED_TIME,AIR_TIME,DISTANCE,CARRIER_DELAY,WEATHER_DELAY,NAS_DELAY,SECURITY_DELAY,LATE_AIRCRAFT_DELAY,Unnamed: 27

# 505798
# 268151
# 232981
# customAggregatedSchemaG = StructType() \
#     .add("OP_CARRIER", StringType()) \
#     .add("NUM_OF_FLIGHTS", IntegerType()) \
#     .add("NUM_OF_DELAYED_FLIGHTS", IntegerType()) \
#     .add("DELAY_ALL_TIME_DEP", IntegerType()) \
#     .add("DELAY_ALL_TIME_ARR", IntegerType()) \
#     .add("TOTAL_CANCELLED", IntegerType()) \
cnt_cond = lambda cond: F.sum(F.when(cond, 1).otherwise(0))

# df2=dfDelaysTotal2013.select([F.when(F.col(c)==,None).otherwise(F.col(c)).alias(c) for c in dfDelaysTotal2013.columns])

# summedDelaysByCarrier2013 = dfDelaysTotal2013.groupBy("OP_CARRIER").sum('CRS_DEP_TIME').orderBy("OP_CARRIER").withColumnRenamed("sum(CRS_DEP_TIME)", "TOTAL_DELAY_ALL_TIME")
summedFlightsByCarrier2013 = dfDelaysTotal2013.groupBy("OP_CARRIER").count().withColumnRenamed("count", "NUM_OF_FLIGHTS")
summedCanceledByCarrier2013 = dfDelaysTotal2013.groupBy('OP_CARRIER').agg(cnt_cond(F.col('CANCELLED') > 0.0).alias('TOTAL_CANCELLED'))
summedDelayOnDeparture2013 = dfDelaysTotal2013.filter(F.col("DEP_DELAY") > 0).groupBy("OP_CARRIER").agg(F.sum("DEP_DELAY"))
summedDelayOnArrival2013 = dfDelaysTotal2013.filter(F.col("ARR_DELAY") > 0).groupBy("OP_CARRIER").agg(F.sum("ARR_DELAY"))
summedDepDelayedFlights2013 = dfDelaysTotal2013.groupBy('OP_CARRIER').agg(cnt_cond(F.col('DEP_DELAY') > 0.0 ).alias("TOTAL_DELAYED_FLIGHTS_ON_DEP"))
summedArrDelayedFlights2013 = dfDelaysTotal2013.groupBy('OP_CARRIER').agg(cnt_cond(F.col('ARR_DELAY') > 0.0 ).alias("TOTAL_DELAYED_FLIGHTS_ON_ARR"))
summedDelayedFlights2013 = dfDelaysTotal2013.filter(~((F.col("DEP_DELAY") == 0) | (F.col("DEP_DELAY") != 0))).groupBy("OP_CARRIER").count().withColumnRenamed("count","TOTAL_DELAYED_FLIGHTS")

# JOINING TABLES TO FORM A SPECIAL ONE

df = summedFlightsByCarrier2013\
.join(summedCanceledByCarrier2013, ["OP_CARRIER"])\
.join(summedDelayedFlights2013,["OP_CARRIER"])\
.join(summedDelayOnDeparture2013,["OP_CARRIER"])\
.join(summedDelayOnArrival2013,["OP_CARRIER"])\
.join(summedDepDelayedFlights2013,["OP_CARRIER"])\
.join(summedArrDelayedFlights2013,["OP_CARRIER"])

df.show()

# print(summedDelayedFlights2013.sum('TOTAL_DELAY_ALL_TIME'))

# summedFlightsByCarrier2013.show()
# summedDelayedFlights2013.show()
# summedDelayedFlights2013.show()


# dfDelaysTotal2013.filter(dfDelaysTotal2013['OP_CARRIER'] == 'UA').filter(dfDelaysTotal2013['DEST'] == 'EWR') \
#                   .filter(dfDelaysTotal2013['CRS_DEP_TIME'] == '1532') \
#                   .filter(dfDelaysTotal2013['FL_DATE'] == '2013-01-11')



# df2.filter(df2['OP_CARRIER'] == 'UA').filter(df2['DEST'] == 'EWR') \
#                   .filter(df2['CRS_DEP_TIME'] == '1532') \
#                   .filter(df2['FL_DATE'] == '2013-01-11').show()





# dfDelaysTotal2013.select(F.col("OP_CARRIER"),F.col("CANCELLED")).show()

# 1.	Na koji način se rangiraju aerodromi kada je u pitanju kašnjenje pri polasku? 
# Sortirati aerodrome prema DEP_DELAY

# 2.	Na koji način se rangiraju aerodromi kada je u pitanju kašnjenje pri dolasku? 
# Sortirati aerodrome prema ARR_DELAY

# 3.	Na koji način se rangiraju aviokompanije kada je u pitanju kašnjenje? 
# Nisam siguran da li treba sortirati aviokompanije samo prema CARRIER_DELAY ili prema zbiru CARRIER_DELAY, WEATHER_DELAY, NAS_DELAY, SECURITY_DELAY, LATE_AIRCRAFT_DELAY

# 4.	Na koji način se rangiraju sami meseci? 
# Ne razumem
# 5.	Koji su najkritičniji periodi u godini za letenje?  Cilj je pokušati utvrditi korelaciju sa nekim praznicima. 
# Ovo vrv mozes gledati i prema aerodromima I prema kompanijama. Mozda najbolje gledati spram ARR DELAY I DEP DELAY, za celu godinu pa izvuci dane. Vrv ce to biti thanksgiving. 4. Jul, bozic itd., a mozda i leto u nekim periodima
# 6.	Koji je najkritičniji dan u nedelji za letenje? 
# Isto preko delay i ARR I DEP, samo na nedeljnom nivou. Mozda ce ti odskakati ove praznicne nedelje, pa mozda treba njih izbaciti.
# 7.	Koji je najgori period u toku dana, sa pragom od 3h? 
# Isto ta dva delaya samo na nivou dana, opet pazi na praznike, nisu karakteristicni
# 8.	Na koji način se rangiraju aviokompanije kada je u pitanju razlika između procenjenog vremena i vremena koje je stvarno proteklo od polaska do dolaska? 
# Razlika CRS ELAPSED TIME I ACTUAL ELAPSED TIME
# 9.	Da li na kašnjenje ikako utiče udaljenost između destinacija? 
# Zar nije obrnuto pitanje? Da li udaljenost utice na kasnjenje? Ako je to pitanje, uzmi u obzir verovatno samo ARR DELAY i obrati paznju da li postoji korelacija izmedju ovog parametra i DISTANCE, ako raste DELAY sa porastom DISTANCE, onda utice.
# 10.	Koja je avio kompanija sa najviše odustanaka u tromesečnom periodu? 
# Vidi CANCELLED pa po kompanijama u tromesecnom periodu
# 11.	Koje su države koje su najgore po pitanju kašnjenja?
# Pogledaj DELAY i ARR i DEP i vidi po stejtovima


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

# dfDelaysTotalYrs.unpersist()


# dfDelaysTotalYrs.show(truncate=False)
# dfDelays2018.show(10)
