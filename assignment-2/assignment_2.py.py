# Databricks notebook source
# MAGIC %md
# MAGIC # Read CSV File and perform transformation 
# MAGIC The task is to calculate the balance of each account number after each transaction using the provided dataset.
# MAGIC The dataset contains the details of all the credits and debits made to accounts daily. 
# MAGIC The objective is to read the given dataset and perform transformation using PySpark code to fulfill this requirement.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Define the Pyspark and Python Imports 

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import when,sum,udf,date_format
from datetime import datetime
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType

# COMMAND ----------

# MAGIC %md
# MAGIC # Declare Sparksession and  App Name

# COMMAND ----------

spark = SparkSession.builder.appName("assignment_credit_debit").getOrCreate()

# COMMAND ----------

schema_assignment = StructType(
    [
        StructField("TransactionsDate", StringType(), True),
        StructField("AccountNumber", StringType(), True),
        StructField("TransactionsType", StringType(), True),
        StructField("Amount", IntegerType(), True),
    ]
)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### The current csv has been uplaoded to path " dbfs:/mnt/assignment-2/ "

# COMMAND ----------

try:
    df_read_csv = spark.read.options(header="True", schema=schema_assignment, delimiter=",").csv("dbfs:/mnt/assignment-2/")
except Exception as e:
    print(f"An error occurred: {e}")
    dbutils.notebook.exit("File not Found !!!!")

# COMMAND ----------

display(df_read_csv.limit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC ### UDF to Convert TransactionsDate from YYYYMMDD to YYYY-MM-DD

# COMMAND ----------

func_datetime = udf(
    lambda var_datetime: datetime.strptime(var_datetime, "%Y%m%d"), DateType()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Windows function variable to  group and calculate cumalative SUM  

# COMMAND ----------

window_partition_order = Window.partitionBy("AccountNumber").orderBy("TransactionsDate").rangeBetween(Window.unboundedPreceding, 0)


# COMMAND ----------

# MAGIC %md 
# MAGIC ### Calculate the Cumalative sum using Pyspark windows function 
# MAGIC ##### Logic :
# MAGIC 1) Credit add the amount and Debit Substract the amount 
# MAGIC 2) If Transaction Type == Credit multiply by +1 and IF Tranaction Type = Debit it reduces the amount therefore mutliply by -1 
# MAGIC 3) Add "NewAmount" column with above logic 
# MAGIC 4) Add Column "CurrentBalance" by Summing  the amount group AccountNumber and Order By TransactionsDate
# MAGIC
# MAGIC

# COMMAND ----------

df_cummulative_sum = (
    df_read_csv.withColumn(
        "NewAmount",
        when(
            df_read_csv.TransactionsType == "Credit",
            (df_read_csv.Amount * 1),
        ).otherwise(df_read_csv.Amount * -1),
    )
    .withColumn("CurrentBalance", sum("NewAmount").over(window_partition_order))
    .drop("NewAmount")
)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Convert TransactionsDate the YYYYMMDD to YYY-MM-DD 
# MAGIC Call Python UDF func_datetime to covert the date from YYYYMMDD to yyyy-MM-dd
# MAGIC

# COMMAND ----------

df_cummulative_final = df_cummulative_sum.withColumn(
    "TransactionsDate",
    date_format(func_datetime(df_cummulative_sum.TransactionsDate), "yyyy-MM-dd"),
)

# COMMAND ----------

display(df_cummulative_final)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Write dataframe df_cummulative to some location 
# MAGIC NOTE : The current data set is to small coalesce has been defined as 1 ,the varaiable coalesce/repartation  can be configured based on performace need and file size  

# COMMAND ----------

### Save to some location in dbfs/AWS/Azure, format csv ,Replace path in save
# df_cummulative_final.coalesce(1).write.format("csv").save("dbfs:/mnt/assignment-2/")


# COMMAND ----------

### Save to table 

# df_cummulative_final.write.saveAsTable("default.assignment_2")

# COMMAND ----------

### Save as External Table 
# df_cummulative_final.write.option('path','dbfs:/mnt/assignment-2/').saveAsTable("default.assignment_2") 
