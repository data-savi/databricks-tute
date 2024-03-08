# Databricks notebook source
error out in run all

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

spark2=SparkSession.builder\
		   .appName("PracticeApplication")\
		   .getOrCreate()
conf=SparkConf()
sc=spark2.sparkContext
print(spark2.conf.get("spark.app.name"))
print(spark2.conf.get("spark.master"))
print(spark)
spark3=spark2.newSession()
print(spark3)

# # rdd1=sc.textfile("")
# rdd1=sc.parallelize([1,2,3,4,5,6,7,8,9,10])
# spark.conf.set("spark2.sql.shuffle.partitions","2")
# rdd1.getNumPartitions()
# # rdd1.getArgument

# import spark.implicits
rdd1=sc.parallelize([('ravi',100),('rinku',50),('ravi',200),('neeta',125),('kavi',150),('kavi',500),('riya',400),('rinku',300)])
print(rdd1.getNumPartitions())
rdd2=rdd1.repartition(1)
data=rdd2.reduceByKey(lambda x,y:x+y)
data.getNumPartitions()
print(data.getNumPartitions())
final_data=data.collect()
print(final_data)
# # final_data.toDF.show()
# for word,count in final_data:
#     print(f"key is {word} and count is {count}")




# COMMAND ----------

sch=["name","sal"]
df1=rdd1.toDF(sch)
df1.printSchema()
df1.show()
# df1.rdd.partitions.size
# [("Finance",10),("Marketing",20),("Sales",30),("IT",40)]

# COMMAND ----------

def f(idx, iterator):
  count = 0
  for _ in iterator:
    count += 1
  return idx, count
rdd1.mapPartitionsWithIndex(f).collect()

def f2(iterator):
  yield sum(1 for _ in iterator)

rdd1.mapPartitions(f2).collect()

# COMMAND ----------

# MAGIC %sql
# MAGIC explain select regexp_extract('abcdef@gmail.com', '^.*@(.*)$', 1)

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,ArrayType,StringType,IntegerType
from pyspark.sql.functions import *
# explode,concat,lit,

data=[(("Neha","Dubey"),["renukoot","prayagraj","delhi"],31,70000),(("Himanshu","Kumar"),["renukoot","ghaziabad","delhi"],32,60000),(("manu","rai"),["gorakhpur","ghaziabad","gurgaon"],31,None)]

schema_data=StructType([
    StructField("name",StructType([
        StructField("fname",StringType(),True),
        StructField("lname",StringType(),True)
])),
    StructField("places",ArrayType(StringType()),True),
    StructField("age",IntegerType(),True),
    StructField("salary",IntegerType(),True)  
])

rdd_1=spark.sparkContext.parallelize(data)
print(rdd_1.getNumPartitions())

df1=spark.createDataFrame(rdd_1,schema=schema_data)
df2=rdd_1.toDF(schema_data)

display(df1)
# display(df2)

# df_3=df1.select(concat(concat(df1.name.lname,lit(', ')), df1.name.fname).alias("name"),explode(df1.places).alias("place"),df1.salary)
# # display(df_3)
# df_3.groupBy("place").agg(sum("salary").alias("summa"),\
# count("*").alias("cnt"))\
# .show()

filling_nulls={"salary":0}
df_filled=df1.na.fill(filling_nulls)
display(df_filled)


# COMMAND ----------

from pyspark.sql.types import StructType,StructField,ArrayType,StringType,IntegerType,MapType
from pyspark.sql.functions import *


# rdd_filtered=df_1.rdd.map(lambda x:(x.name.fname,x.age,x.salary*0.80))
# schema_2=["name","age","sal"]
# df_processed=spark.createDataFrame(rdd_filtered,schema_2)
# df_n=df_processed.filter(((col("name").startswith("m"))) | (col("age")==32))

output=df_n.select(collect_list(col("sal").cast("bigint"))).collect()[0]

data=[(("Neha","Dubey"),["renukoot","prayagraj","delhi"],31,70000,{"dxc":4,"tcs":5}),(("Himanshu","Kumar"),["renukoot","ghaziabad","delhi"],32,60000,{"dxc":7,"teams":1}),(("manu","rai"),["gorakhpur","ghaziabad","gurgaon"],31,90000,{"unicommerce":3,"speed":6})]

schema_data=StructType([
    StructField("name",StructType([
        StructField("fname",StringType(),True),
        StructField("lname",StringType(),True)
]),True),
    StructField("places",ArrayType(StringType()),True),
    StructField("age",IntegerType(),True),
    StructField("salary",IntegerType(),True),
    StructField("company",MapType(StringType(),IntegerType()),True)
])

rdd_1=spark.sparkContext.parallelize(data)
print(rdd_1.getNumPartitions())

df_1=rdd_1.toDF(schema_data)
# display(df_1.company['tcs'])
rdd_filtered=df_1.rdd.map(lambda x:(x.name.fname,x.age,x.salary*0.80))
schema_2=["name","age","sal"]
df_processed=spark.createDataFrame(rdd_filtered,schema_2)
df_n=df_processed.filter(((col("name").startswith("m"))) | (col("age")==32))

output=df_n.select(collect_list(col("sal").cast("bigint"))).collect()[0]

print(output)
for x in output:
    print(x)
    # print(y)




# COMMAND ----------

# MAGIC %md when otherwise , case when
# MAGIC

# COMMAND ----------

data=[(1,"Abc",23000),(2,"Bcd",20000),(3,"Cde",15000)]
sch=["id","name","sal"]
df1=spark.createDataFrame(data,schema=sch)

##############  Logic
# when(condition1,value1).when(condition2,value2).otherwise(default)
# select(expr(""" case when condition then value
#                      when condition2 then value2
#                      esle default end """))

df2=df1.withColumn("band",when((df1.sal >= 15000) & (df1.sal <20000 ),"C")\
                        .when(((df1.sal) >= 20000) & (df1.sal <25000),"B")\
                            .when((df1.sal) > 25000, "A")\
                                .otherwise("NA"))
display(df2)
print("---------------------------")
df3=df1.select("*",expr("case when sal >= 15000 and sal  < 20000 then 'C'"+\
                        " when sal >= 20000 and sal <25000 then 'B' "+\
                        " when sal > 25000 then 'A'" +\
                        " else 'NA' end").alias("band"))
display(df3)
print("---------------------------")
df4=df1.select("*",expr("""case when sal >= 15000 and sal  < 20000 then 'C' 
                                when sal >= 20000 and sal <25000 then 'B'  
                                when sal > 25000 then 'A' 
                                else 'NA' end""").alias("band"))

display(df4)

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,ArrayType,StringType,IntegerType,MapType
from pyspark.sql.functions import *

data=[(("Neha","Dubey"),["renukoot","prayagraj","delhi"],31,70000,{"dxc":4,"tcs":5}),(("Himanshu","Kumar"),["renukoot","ghaziabad","delhi"],32,60000,{"dxc":7,"teams":1}),(("manu","rai"),["gorakhpur","ghaziabad","gurgaon"],31,90000,{"unicommerce":3,"speed":6})]

schema_data=StructType([
    StructField("name",StructType([
        StructField("fname",StringType(),True),
        StructField("lname",StringType(),True)
]),True),
    StructField("places",ArrayType(StringType()),True),
    StructField("age",IntegerType(),True),
    StructField("salary",IntegerType(),True),
    StructField("company",MapType(StringType(),IntegerType()),True)
])

rdd_1=spark.sparkContext.parallelize(data)
print(rdd_1.getNumPartitions())

df_1=rdd_1.toDF(schema_data)
# display(df_1.company['tcs'])
rdd_filtered=df_1.rdd.map(lambda x:(x.name.fname,x.age,x.salary*0.80))
schema_2=["name","age","sal"]
df_processed=spark.createDataFrame(rdd_filtered,schema_2)

# 3 eways to do filter================================>>>>>>>>>>>

df_n=df_processed.filter(((col("name").startswith("m"))) | (col("age")==32))
display(df_n)
print("-----------------------------")
df_n1=df_processed.filter(expr("name like 'm%' or age=32"))
display(df_n1)

print("-----------------------------")
df_n2=df_processed.filter((df_processed.name.startswith('m')) | (df_processed.age==32))
display(df_n2)
# Important to understand usability of withColumn vs Select ====================================>>>>>>>>>>

display(df_1.withColumn('places_str',concat_ws(';',df_1.places)))
display(df_1.select("*",expr("concat_ws(':',places)").alias("places_str_str")))


# .alias("places_str")))




# COMMAND ----------

# MAGIC %md Date Manipulations
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %md Window Functions
# MAGIC

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import *

data=[(1,"Riya","sales",20000),(2,"Riyana","sales",200),(2,"Priya","finance",2000),(4,"Riyank","IT",40000),(5,"Priyanka","sales",200)]
schema_1=["id","name","dept","sal"]

rdd1=d1=spark.sparkContext.parallelize(data)
rdd1.coalesce(1)
df_1=rdd1.toDF(schema_1)

win_spec=Window.partitionBy("dept").orderBy(col("sal").desc())
df2=df_1.select("dept","sal").withColumn("row_number",rank().over(win_spec))
# df2=df_1.select(first(col("sal")).over(win_spec).alias("row_n")).select(max(col("row_n")))
display(df2)

win_spec1=Window.orderBy("dept").rowsBetween(Window.unboundedPreceding,Window.currentRow)
df3=df2.withColumn("sum_preceeding",sum("sal").over(win_spec1))
display(df3)

# display(df2.filter(expr("row_number = 1")))
# display(df2.filter(df2.row_number==1))
# 

# COMMAND ----------

# DBTITLE 1,regexp_replace
address = [(1,"14851 Jeffrey Rd","DE"),
    (2,"43421 Margarita St","NY"),
    (3,"13111 Siemon Ave","CA")]
df =spark.createDataFrame(address,["id","address","state"])
df.show()

stateDic={'CA':'California','NY':'New York','DE':'Delaware'}
cols=["id","address","state"]
df2=df.rdd.map(lambda x: 
    (x.id,x.address,stateDic[x.state]) 
    ).toDF(cols)
df2.show()

# COMMAND ----------


from pyspark.sql.types import *
# data=[(1,"abc",2300,2023-12-23)]
# sch1=["id","name","sal","date_p"]

sch=StructType()\
.add("id",StringType(),True)\
.add("name",StringType(),True)\
.add("sal",IntegerType(),True)\
.add("date_processed",DateType(),True)

df1=spark.read.format("csv").schema(sch).load("/FileStore/tables/test_csv.txt")
display(df1)
df1.write.format("parquet").parquet("/FileStore/tables/parquet_data/abc.parquet")
df1.write.format("csv").csv("/FileStore/tables/csv_data/abc.csv")
# display(spark.createDataFrame(data,sch))

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/parquet_data/abc.parquet