#required imports for running the spark program

from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark.sql.functions import col
from pyspark.sql import HiveContext


#creating hive context to fetch data from hive tables 
hive_context = HiveContext(sc)

#loading the data from hive tables into spark dataframes 
df_local_finance = hive_context.table("murthy_project.local_finance")
df_central_finance = hive_context.table("murthy_project.central_finance")
df_general_finance = hive_context.table("murthy_project.general_finance")

#converting data frame to temporary tables 
df_local_finance.createOrReplaceTempView("spark_tab_local")
df_central_finance.createOrReplaceTempView("spark_tab_central")
df_general_finance.createOrReplaceTempView("spark_tab_general")

#Query-01
#Aggregate operation on local finance for the similar groups,series_reference,status to perform a total on the data_values for statuses FINAL and REVISED 

query_01 = open("/home/reallegendscorp9155/murthy_project/query_files/query_01.txt")
query01 = query_01.read()
df_q1 = sqlContext.sql(query01).filter(col('status').isin(['FINAL','REVISED']) == True) 
df_q1.createOrReplaceTempView("temp_tab01")


#Query-02
#Average of data_value is taken among the local, general and central finance tables and formatting is done to reduce the precision on the value obtained

query_02 = open("/home/reallegendscorp9155/murthy_project/query_files/query_02.txt")
query02 = query_02.read()
df2 = sqlContext.sql(query02).groupBy("Series_title_1").avg("data_value")
df2_new =  df2.withColumnRenamed("avg(data_value)","avd_value")
from pyspark.sql.functions import avg, format_number 
df_q2=df2_new.select(['Series_title_1',format_number('avd_value', 3).alias("avg_data_value")])
df_q2.createOrReplaceTempView("temp_tab02")


#Query-03
#Join operation is made  among the local, general and central finance tables to get the matching records and their data_value's sum

query_03 = open("/home/reallegendscorp9155/murthy_project/query_files/query_03.txt")
query03 = query_03.read()
df_q3 = sqlContext.sql(query03)
df_q3.withColumnRenamed("avg(data_value)","avd_value")
df_q3.withColumnRenamed("avg(data_value)","avd_value")
df_q3.withColumnRenamed("avg(data_value)","avd_value")
df_q3.createOrReplaceTempView("temp_tab03")


#transformed data is loaded in hive tables 
hive_context.sql("DROP TABLE IF EXISTS murthy_project.htab01")
hive_context.sql("create table murthy_project.htab01  as select * from temp_tab01");
hive_context.sql("DROP TABLE IF EXISTS murthy_project.htab02")
hive_context.sql("create table murthy_project.htab02  as select * from temp_tab02");
hive_context.sql("DROP TABLE IF EXISTS murthy_project.htab03")
hive_context.sql("create table murthy_project.htab03  as select * from temp_tab03");
