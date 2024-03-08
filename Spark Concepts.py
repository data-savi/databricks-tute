# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #this includes all Spark concepts#
# MAGIC ##we will learn create dataframe, transaformations, actions##
# MAGIC **make sure to understand the shuffles** 
# MAGIC
# MAGIC
# MAGIC *hello*
# MAGIC
# MAGIC ~~get rid of old concepts~~
# MAGIC
# MAGIC 1. new value
# MAGIC 1. new value2
# MAGIC 1. new value3
# MAGIC
# MAGIC
# MAGIC - new1
# MAGIC - new2
# MAGIC - new3

# COMMAND ----------

from pyspark.sql.types import StringType,IntegerType,BooleanType,ArrayType,MapType,StructType,StructField
from pyspark.sql.functions import *


sc=spark.sparkContext
# print(sc)
sc.setJobDescription("Step 1 : Job")

# data=[(1,("kumari","riya")),(2,("pandey","priya")),(3,("kumari","sneha"))]
data_1=[(1,"kumari;riya;kumari"),(2,"pandey;priya"),(3,"kumari;sneha")]
schema=["id","name"]
# schema_1=StructType([
#     StructField("id",IntegerType(),True),
#     StructField("name",StructType([
#         StructField("lname",StringType(),True),
#         StructField("fname",StringType(),True)
# ]),True)
# ])
df=spark.createDataFrame(data_1,schema)
# df_1=spark.createDataFrame(data_1,schema_1)
display(df)

#### Option - 1 : using withColumn
df_1=df.withColumn("name_arry",split(df.name,';'))
# display(df_1)
# df_2=df_1.withColumn("exploded_column",explode(df_1.name_arry)).select("id",,"exploded_column")
# display(df_2)
# df_3=df_1.select("id",expr("posexplode(name_arry)").alias("position").alias("exploded_name"))
df_3=df_1.select("id",posexplode(df_1.name_arry))
# display(df_3)

#### Option - 2 : using Select without expr
# df_s_1=df.select("id",split(col("name"),";").alias("name_arry"))
# display(df_s_1)
# df_s_2=df_s_1.select("id",explode(col("name_arry")).alias("name_exploded"))
# display(df_s_2)

#### Option - 3 : using Select with expr
# df_s_1=df.select("id",expr("split(name,';') as name_arry"))
# display(df_s_1)
# df_s_2=df_s_1.select("id",expr("explode(name_arry) as name_exploded"))
# display(df_s_2)

#### Option - 4 : using rdd and Lambda

# rdd_lamb_1=df.rdd.map(lambda x:(x[0],x[1].split(';')))
# rdd_lamb_2=rdd_lamb_1.map(lambda x:(x[0],lit(explode(x[1]))))
# df_lamb_2=rdd_lamb_2.toDF(["lamb_id","lamb_exploded_name"])

# display(df_lamb_2)

# display(df_s_2.groupBy("id").pivot("name_exploded").count())

# COMMAND ----------

# Json data

df_json=spark.read.format("json").load("/FileStore/tables/zipcodes.json")
# display(df_json)
df_schema=spark.createDataFrame(df_json.dtypes,["col","type_d"])
data=df_schema.filter(df_schema.type_d.isin('string','bigint')).select("col").rdd.flatMap(lambda x :x).collect()

cols=','.join(data) 
print(cols)




# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import *
# def abc(x):
#     ''' the func prints hello for user '''
#     print(f'Hello {x.upper()}')
# abc("savinay")



# print(explode.__doc__)
print(help(explode_outer))

# COMMAND ----------

from pyspark.sql.types import StringType,IntegerType,BooleanType,ArrayType,MapType,StructType,StructField
from pyspark.sql.functions import *


sc=spark.sparkContext
# print(sc)
sc.setJobDescription("Step 1 : Job")

# data=[(1;("kumari","riya")),(2;("pandey","priya")),(3;("kumari","sneha"))]
data=[(1;"Hello"),(2;"World"),(3;"Planet")]
# data_1=[(1,"kumari;riya;kumari"),(2,"pandey;priya"),(3,"kumari;sneha")]
schema=["id","name"]



# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window

sc=spark.sparkContext
sc.setJobDescription("hello: Step-1")

data=[(1,"ABC"),(2,"XYZ")]
sch=["id","name"]

df_1=spark.createDataFrame(data,sch)
display(df_1)

# df_2=spark.read.format("csv").option("inferSchema","True").option("hearder","True").load("/*.csv")
# display(df_2)


df_3=df_1.withColumn("startswith",when(df_1.name.startswith('A'),"A_Name")\
        .when(df_1.name.startswith('Z'),"P_Name")\
            .otherwise("Y_Name"))
display(df_3)

# COMMAND ----------

data=[(1,"Ind"),(2,"Aus"),(3,"Nz"),(4,"Pak")]
sch=["id","team"]
df_1=spark.createDataFrame(data,sch)
df_1.createOrReplaceGlobalTempView("matches")
display(spark.sql("select m1.team || ' vs ' || m2.team from global_temp.matches m1 join global_temp.matches m2 on m1.team<m2.team"))