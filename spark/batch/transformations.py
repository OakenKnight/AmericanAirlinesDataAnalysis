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


cnt_cond = lambda cond: F.sum(F.when(cond, 1).otherwise(0))

# PART FOR AIRPLANE COMPANIES
# print("+++++++++++++++++++++ CREATING DF FOR AIRPLANE COMPANIES +++++++++++++++++++++")
# summedFlightsByCarrier2013 = dfDelaysTotal2013.groupBy("OP_CARRIER").count().withColumnRenamed("count", "NUM_OF_FLIGHTS")
# summedCanceledByCarrier2013 = dfDelaysTotal2013.groupBy('OP_CARRIER').agg(cnt_cond(F.col('CANCELLED') > 0.0).alias('TOTAL_CANCELLED'))
# summedDelayedFlights1stQuarter2013ByCarrier = dfDelaysTotal2013.filter(((F.col("FL_DATE") >= '2013-01-01') & (F.col("FL_DATE") <= '2013-03-31') & (F.col("CANCELLED")>0.0))).groupBy("OP_CARRIER").count().withColumnRenamed("count","TOTAL_CANCELLED_1st_QUARTAL")
# summedDelayedFlights2ndQuarter2013ByCarrier = dfDelaysTotal2013.filter(((F.col("FL_DATE") >= '2013-04-01') & (F.col("FL_DATE") <= '2013-06-30') & (F.col("CANCELLED")>0.0))).groupBy("OP_CARRIER").count().withColumnRenamed("count","TOTAL_CANCELLED_2nd_QUARTAL")
# summedDelayedFlights3rdQuarter2013ByCarrier = dfDelaysTotal2013.filter(((F.col("FL_DATE") >= '2013-07-01') & (F.col("FL_DATE") <= '2013-09-30') & (F.col("CANCELLED")>0.0))).groupBy("OP_CARRIER").count().withColumnRenamed("count","TOTAL_CANCELLED_3rd_QUARTAL")
# summedDelayedFlights4thQuarter2013ByCarrier = dfDelaysTotal2013.filter(((F.col("FL_DATE") >= '2013-10-01') & (F.col("FL_DATE") <= '2013-12-31') & (F.col("CANCELLED")>0.0))).groupBy("OP_CARRIER").count().withColumnRenamed("count","TOTAL_CANCELLED_4th_QUARTAL")
# summedDelayOnDeparture2013 = dfDelaysTotal2013.filter(F.col("DEP_DELAY") > 0).groupBy("OP_CARRIER").agg(F.sum("DEP_DELAY").alias('SUMMED_DELAY_ON_DEP'))
# summedDelayOnArrival2013 = dfDelaysTotal2013.filter(F.col("ARR_DELAY") > 0).groupBy("OP_CARRIER").agg(F.sum("ARR_DELAY").alias('SUMMED_DELAY_ON_ARR'))
# summedDepDelayedFlights2013 = dfDelaysTotal2013.groupBy('OP_CARRIER').agg(cnt_cond(F.col('DEP_DELAY') > 0.0 ).alias("TOTAL_DELAYED_FLIGHTS_ON_DEP"))
# summedArrDelayedFlights2013 = dfDelaysTotal2013.groupBy('OP_CARRIER').agg(cnt_cond(F.col('ARR_DELAY') > 0.0 ).alias("TOTAL_DELAYED_FLIGHTS_ON_ARR"))
# summedDelayedFlights2013 = dfDelaysTotal2013.filter(((F.col("DEP_DELAY") > 0) | (F.col("ARR_DELAY") > 0))).groupBy("OP_CARRIER").count().withColumnRenamed("count","TOTAL_DELAYED_FLIGHTS")
# print("+++++++++++++++++++++ DONE +++++++++++++++++++++")


# PART FOR AIRPLANE COMPANIES FOR TIME OF DAY
# print("+++++++++++++++++++++ CREATING DF FOR AIRPLANE COMPANIES FOR 4h PERIODS +++++++++++++++++++++")
# delayByCarrier0_4 = dfDelaysTotal2013.filter(((F.col("DEP_TIME")>=0000) & (F.col("DEP_TIME")<=400) & ( (F.col("DEP_DELAY")>0.0) | (F.col("ARR_DELAY")>0.0)))).groupBy("OP_CARRIER").count().withColumnRenamed("count","DELAY_0_4")
# delayByCarrier4_8 = dfDelaysTotal2013.filter(((F.col("DEP_TIME")>400) & (F.col("DEP_TIME")<=800) & ( (F.col("DEP_DELAY")>0.0) | (F.col("ARR_DELAY")>0.0)) )).groupBy("OP_CARRIER").count().withColumnRenamed("count","DELAY_4_8")
# delayByCarrier8_12 = dfDelaysTotal2013.filter(((F.col("DEP_TIME")>800)& (F.col("DEP_TIME")<=1200) & ((F.col("DEP_DELAY")>0.0) | (F.col("ARR_DELAY")>0.0)))).groupBy("OP_CARRIER").count().withColumnRenamed("count","DELAY_8_12")
# delayByCarrier12_16 = dfDelaysTotal2013.filter(((F.col("DEP_TIME")>1200) & (F.col("DEP_TIME")<=1600) & ((F.col("DEP_DELAY")>0.0) | (F.col("ARR_DELAY")>0.0)))).groupBy("OP_CARRIER").count().withColumnRenamed("count","DELAY_12_16")
# delayByCarrier16_20 = dfDelaysTotal2013.filter(((F.col("DEP_TIME")>1600)& (F.col("DEP_TIME")<=2000) &( (F.col("DEP_DELAY")>0.0) | (F.col("ARR_DELAY")>0.0)))).groupBy("OP_CARRIER").count().withColumnRenamed("count","DELAY_16_20")
# delayByCarrier20_24 = dfDelaysTotal2013.filter(((F.col("DEP_TIME")>2000) & (F.col("DEP_TIME")<=2359)&( (F.col("DEP_DELAY")>0.0) | (F.col("ARR_DELAY")>0.0)))).groupBy("OP_CARRIER").count().withColumnRenamed("count","DELAY_20_24")
# summedDelayedFlights2013 = dfDelaysTotal2013.filter(((F.col("DEP_DELAY") > 0) | (F.col("ARR_DELAY") > 0))).groupBy("OP_CARRIER").count().withColumnRenamed("count","TOTAL_DELAYED_FLIGHTS")
# print("+++++++++++++++++++++ DONE +++++++++++++++++++++")



# PART FOR AIRPORTS
# print("+++++++++++++++++++++ CREATING DF FOR ORIGIN AIRPORTS +++++++++++++++++++++")
# summedDEPDelayFlightsOnAirport = dfDelaysTotal2013.groupBy('ORIGIN').agg(cnt_cond(F.col('DEP_DELAY') > 0.0).alias('TOTAL_DELAYED_ON_DEP')).orderBy("ORIGIN")
# print("+++++++++++++++++++++ DONE +++++++++++++++++++++")
# print("+++++++++++++++++++++ CREATING DF FOR DESTINATION AIRPORTS +++++++++++++++++++++")
# summedARRDelayFlightsOnAirport = dfDelaysTotal2013.groupBy('DEST').agg(cnt_cond(F.col('ARR_DELAY') > 0.0).alias('TOTAL_DELAYED_ON_ARR')).orderBy("DEST")
# print("+++++++++++++++++++++ DONE +++++++++++++++++++++")



# JOINING TABLES TO FORM A SPECIAL ONE
# print("+++++++++++++++++++++ JOINTING DFS FOR AIRPLANE COMPANIES TABLE +++++++++++++++++++++")
# df = summedFlightsByCarrier2013\
# .join(summedCanceledByCarrier2013, ["OP_CARRIER"])\
# .join(summedDelayedFlights1stQuarter2013ByCarrier,['OP_CARRIER'])\
# .join(summedDelayedFlights2ndQuarter2013ByCarrier,['OP_CARRIER'])\
# .join(summedDelayedFlights3rdQuarter2013ByCarrier,['OP_CARRIER'])\
# .join(summedDelayedFlights4thQuarter2013ByCarrier,['OP_CARRIER'])\
# .join(summedDelayedFlights2013,["OP_CARRIER"])\
# .join(summedDelayOnDeparture2013,["OP_CARRIER"])\
# .join(summedDelayOnArrival2013,["OP_CARRIER"])\
# .join(summedDepDelayedFlights2013,["OP_CARRIER"])\
# .join(summedArrDelayedFlights2013,["OP_CARRIER"])
# df.show()
# print("+++++++++++++++++++++ DONE +++++++++++++++++++++")



# JOINING TABLES TO FORM A SPECIAL ONE
# print("+++++++++++++++++++++ JOINTING DFS FOR AIRPLANE COMPANIES 4H PERIODS OF DAY TABLE  +++++++++++++++++++++")
# df_delays_for_days = delayByCarrier0_4\
#     .join(delayByCarrier4_8,["OP_CARRIER"])\
#     .join(delayByCarrier8_12,["OP_CARRIER"])\
#     .join(delayByCarrier12_16,["OP_CARRIER"])\
#     .join(delayByCarrier16_20,["OP_CARRIER"])\
#     .join(delayByCarrier20_24,["OP_CARRIER"])\
#     .join(summedDelayedFlights2013,["OP_CARRIER"])

# df_delays_for_days.show()
# print("+++++++++++++++++++++ DONE +++++++++++++++++++++")




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

# NO_DATA
# 5.	Koji su najkritičniji periodi u godini za letenje?  Cilj je pokušati utvrditi korelaciju sa nekim praznicima. 
# Ovo vrv moze se gledati i prema aerodromima I prema kompanijama. Mozda najbolje gledati spram ARR DELAY I DEP DELAY, za celu godinu pa izvuci dane. Vrv ce to biti thanksgiving. 4. Jul, bozic itd., a mozda i leto u nekim periodima
# 6.	Koji je najkritičniji dan u nedelji za letenje? 
# Isto preko delay i ARR I DEP, samo na nedeljnom nivou. Mozda ce odskakati ove praznicne nedelje, pa mozda treba njih izbaciti.


# TODO:
# 8.	Na koji način se rangiraju aviokompanije kada je u pitanju razlika između procenjenog vremena i vremena koje je stvarno proteklo od polaska do dolaska? 
# Razlika CRS ELAPSED TIME I ACTUAL ELAPSED TIME


# HAS TO COMBINE ANOTHER TABLE, MAYBE TOO MUCH FOR FIRST PHASE
#druga tabela
# 11.	Koje su države koje su najgore po pitanju kašnjenja?
# Pogledaj DELAY i ARR i DEP i vidi po stejtovima


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
