#!/usr/bin/python

import os
from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as F

from pyspark.sql.types import *

Hdf_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

cnt_cond = lambda cond: F.sum(F.when(cond, 1).otherwise(0))

# BEGGINING OF CURRATED ZONE

sparkMongo = SparkSession.builder.appName("currated-application") \
    .config("spark.mongodb.output.uri", "mongodb://mongodb:27017/currated-data") \
    .getOrCreate()

# while True:
#     try:
#         dfDelaysTotalYrs = sparkMongo.read.option("multiline", "true").option("sep", ",").option("header", "true") \
#         .option("inferSchema", "true").csv(Hdf_NAMENODE + "/transformation_layer/dfDelaysTotalYrs")
#         print("\n\n<<<<<<<<< CURATED COMPONENT READ dfDelaysTotalYrs >>>>>>>>>>>\n\n")
#         break
#     except:
#         print("<<<<<<<<<<<<<<<<< Failure reading! Retrying... >>>>>>>>>>>>>>>>>")
#         time.sleep(5)

while True:
    try:
        dfAirportsAndDelays = sparkMongo.read.option("multiline", "true").option("sep", ",").option("header", "true") \
        .option("inferSchema", "true").csv(Hdf_NAMENODE + "/transformation_layer/dfAirportsAndDelays")
        print("\n\n<<<<<<<<< CURATED COMPONENT READ dfAirportsAndDelays >>>>>>>>>>>\n\n")
        break
    except:
        print("<<<<<<<<<<<<<<<<< Failure reading! Retrying... >>>>>>>>>>>>>>>>>")
        time.sleep(5)

# PART FOR AIRPLANE COMPANIES

# print("+++++++++++++++++++++ CREATING DF FOR AIRPLANE COMPANIES +++++++++++++++++++++")


# summedFlightsByCarrier2013 = dfDelaysTotalYrs.groupBy("OP_CARRIER").count().withColumnRenamed("count", "NUM_OF_FLIGHTS")
# summedCanceledByCarrier2013 = dfDelaysTotalYrs.groupBy('OP_CARRIER').agg(cnt_cond(F.col('CANCELLED') > 0.0).alias('TOTAL_CANCELLED'))
# summedDelayedFlights1stQuarter2013ByCarrier = dfDelaysTotalYrs.filter(((F.col("FL_DATE") >= '2013-01-01') & (F.col("FL_DATE") <= '2013-03-31') & (F.col("CANCELLED")>0.0))).groupBy("OP_CARRIER").count().withColumnRenamed("count","TOTAL_CANCELLED_1st_QUARTAL")
# summedDelayedFlights2ndQuarter2013ByCarrier = dfDelaysTotalYrs.filter(((F.col("FL_DATE") >= '2013-04-01') & (F.col("FL_DATE") <= '2013-06-30') & (F.col("CANCELLED")>0.0))).groupBy("OP_CARRIER").count().withColumnRenamed("count","TOTAL_CANCELLED_2nd_QUARTAL")
# summedDelayedFlights3rdQuarter2013ByCarrier = dfDelaysTotalYrs.filter(((F.col("FL_DATE") >= '2013-07-01') & (F.col("FL_DATE") <= '2013-09-30') & (F.col("CANCELLED")>0.0))).groupBy("OP_CARRIER").count().withColumnRenamed("count","TOTAL_CANCELLED_3rd_QUARTAL")
# summedDelayedFlights4thQuarter2013ByCarrier = dfDelaysTotalYrs.filter(((F.col("FL_DATE") >= '2013-10-01') & (F.col("FL_DATE") <= '2013-12-31') & (F.col("CANCELLED")>0.0))).groupBy("OP_CARRIER").count().withColumnRenamed("count","TOTAL_CANCELLED_4th_QUARTAL")
# summedDelayOnDeparture2013 = dfDelaysTotalYrs.filter(F.col("DEP_DELAY") > 0).groupBy("OP_CARRIER").agg(F.sum("DEP_DELAY").alias('SUMMED_DELAY_ON_DEP'))
# summedDelayOnArrival2013 = dfDelaysTotalYrs.filter(F.col("ARR_DELAY") > 0).groupBy("OP_CARRIER").agg(F.sum("ARR_DELAY").alias('SUMMED_DELAY_ON_ARR'))
# summedDepDelayedFlights2013 = dfDelaysTotalYrs.groupBy('OP_CARRIER').agg(cnt_cond(F.col('DEP_DELAY') > 0.0 ).alias("TOTAL_DELAYED_FLIGHTS_ON_DEP"))
# summedArrDelayedFlights2013 = dfDelaysTotalYrs.groupBy('OP_CARRIER').agg(cnt_cond(F.col('ARR_DELAY') > 0.0 ).alias("TOTAL_DELAYED_FLIGHTS_ON_ARR"))
# summedDelayedFlights2013 = dfDelaysTotalYrs.filter(((F.col("DEP_DELAY") > 0) | (F.col("ARR_DELAY") > 0))).groupBy("OP_CARRIER").count().withColumnRenamed("count","TOTAL_DELAYED_FLIGHTS")
# summed_differences_crs_time_and_time_2013 = dfDelaysTotalYrs.select(((F.col("DEP_TIME") - F.col("CRS_DEP_TIME"))).alias("DIFFERENCE_BETWEEN_FLIGHTS"), F.col("OP_CARRIER")).filter(F.col("DIFFERENCE_BETWEEN_FLIGHTS")>0).groupBy("OP_CARRIER").mean("DIFFERENCE_BETWEEN_FLIGHTS")

# print("+++++++++++++++++++++ DONE +++++++++++++++++++++")

# # PART FOR AIRPLANE COMPANIES FOR TIME OF DAY
# print("+++++++++++++++++++++ CREATING DF FOR AIRPLANE COMPANIES FOR 4h PERIODS +++++++++++++++++++++")
# delayByCarrier0_4 = dfDelaysTotalYrs.filter(((F.col("DEP_TIME")>=0000) & (F.col("DEP_TIME")<=400) & ( (F.col("DEP_DELAY")>0.0) | (F.col("ARR_DELAY")>0.0)))).groupBy("OP_CARRIER").count().withColumnRenamed("count","DELAY_0_4")
# delayByCarrier4_8 = dfDelaysTotalYrs.filter(((F.col("DEP_TIME")>400) & (F.col("DEP_TIME")<=800) & ( (F.col("DEP_DELAY")>0.0) | (F.col("ARR_DELAY")>0.0)) )).groupBy("OP_CARRIER").count().withColumnRenamed("count","DELAY_4_8")
# delayByCarrier8_12 = dfDelaysTotalYrs.filter(((F.col("DEP_TIME")>800)& (F.col("DEP_TIME")<=1200) & ((F.col("DEP_DELAY")>0.0) | (F.col("ARR_DELAY")>0.0)))).groupBy("OP_CARRIER").count().withColumnRenamed("count","DELAY_8_12")
# delayByCarrier12_16 = dfDelaysTotalYrs.filter(((F.col("DEP_TIME")>1200) & (F.col("DEP_TIME")<=1600) & ((F.col("DEP_DELAY")>0.0) | (F.col("ARR_DELAY")>0.0)))).groupBy("OP_CARRIER").count().withColumnRenamed("count","DELAY_12_16")
# delayByCarrier16_20 = dfDelaysTotalYrs.filter(((F.col("DEP_TIME")>1600)& (F.col("DEP_TIME")<=2000) &( (F.col("DEP_DELAY")>0.0) | (F.col("ARR_DELAY")>0.0)))).groupBy("OP_CARRIER").count().withColumnRenamed("count","DELAY_16_20")
# delayByCarrier20_24 = dfDelaysTotalYrs.filter(((F.col("DEP_TIME")>2000) & (F.col("DEP_TIME")<=2359)&( (F.col("DEP_DELAY")>0.0) | (F.col("ARR_DELAY")>0.0)))).groupBy("OP_CARRIER").count().withColumnRenamed("count","DELAY_20_24")
# summedDelayedFlights2013 = dfDelaysTotalYrs.filter(((F.col("DEP_DELAY") > 0) | (F.col("ARR_DELAY") > 0))).groupBy("OP_CARRIER").count().withColumnRenamed("count","TOTAL_DELAYED_FLIGHTS")
# print("+++++++++++++++++++++ DONE +++++++++++++++++++++")



# PART FOR AIRPORTS
print("+++++++++++++++++++++ CREATING DF FOR ORIGIN AIRPORTS +++++++++++++++++++++")
summedDEPDelayFlightsOnAirport = dfAirportsAndDelays.groupBy('ORIGIN').agg(cnt_cond(F.col('DEP_DELAY') > 0.0).alias('TOTAL_DELAYED_ON_DEP')).orderBy("ORIGIN")
meanDEPDelayFlightsOnAirport = dfAirportsAndDelays.filter(F.col("DEP_DELAY") > 0).groupBy('ORIGIN').mean("DEP_DELAY").withColumnRenamed("avg(DEP_DELAY)","AVG_DEP_DELAY")
df_airport_dep_delays = summedDEPDelayFlightsOnAirport.join(meanDEPDelayFlightsOnAirport, ["ORIGIN"])
df_airport_dep_delays.show()
print("+++++++++++++++++++++ DONE +++++++++++++++++++++")

print("+++++++++++++++++++++ CREATING DF FOR DESTINATION AIRPORTS +++++++++++++++++++++")
summedARRDelayFlightsOnAirport = dfAirportsAndDelays.groupBy('DEST').agg(cnt_cond(F.col('ARR_DELAY') > 0.0).alias('TOTAL_DELAYED_ON_ARR')).orderBy("DEST")
meanARRDelayFlightsOnAirport = dfAirportsAndDelays.groupBy('DEST').filter(F.col("ARR_DELAY") > 0).mean("ARR_DELAY").withColumnRenamed("avg(ARR_DELAY)","AVG_ARR_DELAY")

df_airport_arr_delays = summedARRDelayFlightsOnAirport.join(meanARRDelayFlightsOnAirport, ["ORIGIN"])
df_airport_arr_delays.show()
print("+++++++++++++++++++++ DONE +++++++++++++++++++++")



# # JOINING TABLES TO FORM A SPECIAL ONE
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
# .join(summedArrDelayedFlights2013,["OP_CARRIER"])\
# .join(summed_differences_crs_time_and_time_2013, ["OP_CARRIER"])
# print("+++++++++++++++++++++ DONE +++++++++++++++++++++")



# # JOINING TABLES TO FORM A SPECIAL ONE
# print("+++++++++++++++++++++ JOINTING DFS FOR AIRPLANE COMPANIES 4H PERIODS OF DAY TABLE  +++++++++++++++++++++")
# df_delays_for_days = delayByCarrier0_4\
#     .join(delayByCarrier4_8,["OP_CARRIER"])\
#     .join(delayByCarrier8_12,["OP_CARRIER"])\
#     .join(delayByCarrier12_16,["OP_CARRIER"])\
#     .join(delayByCarrier16_20,["OP_CARRIER"])\
#     .join(delayByCarrier20_24,["OP_CARRIER"])\
#     .join(summedDelayedFlights2013,["OP_CARRIER"])

# print("+++++++++++++++++++++ DONE +++++++++++++++++++++")


# df.write.format("mongo").mode("overwrite").option("database",
# "currated-data").option("collection", "airplane-companies").save()

# summedDEPDelayFlightsOnAirport.write.format("mongo").mode("overwrite").option("database",
# "currated-data").option("collection", "summed-dep-airports").save()

# summedARRDelayFlightsOnAirport.write.format("mongo").mode("overwrite").option("database",
# "currated-data").option("collection", "summed-arr-airports").save()


# df_delays_for_days.write.format("mongo").mode("overwrite").option("database",
# "currated-data").option("collection", "airplane-days").save()

# print("+++++++++++++++++++++ DONE WITH CURRATED ZONE+++++++++++++++++++++")

# END OF CURRATED ZONE

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

