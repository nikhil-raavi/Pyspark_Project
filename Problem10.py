'''
𝐐𝐮𝐞𝐬𝐭𝐢𝐨𝐧
Given a dataset of monthly sales records with salespeople names and their regions,
calculate the month with the highest sales for each region using window functions and
the max() function. Ensure that the result includes the region name, month, and sales value.
Consider sales fluctuations, and the dataset should contain multiple records for each region to
test windowing correctly.
'''
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('Pyspark Practice').config('key','value').getOrCreate()

data = [ ("Amit", "North", "Jan", 12000), ("Rajesh", "North", "Feb", 15000), ("Sunita", "North", "Mar", 11000), ("Meena", "South", "Jan", 17000),
("Ravi", "South", "Feb", 20000), ("Priya", "South", "Mar", 18000),
("Suresh", "East", "Jan", 10000), ("Vishal", "East", "Feb", 22000),
("Akash", "East", "Mar", 21000), ("Anjali", "West", "Jan", 15000),
("Deepak", "West", "Feb", 13000), ("Nidhi", "West", "Mar", 17000), ]

columns = ["Salesperson", "Region", "Month", "Sales"]

df = spark.createDataFrame(data,columns)

window_spec = Window.partitionBy('Region').orderBy(desc('Sales'))
window_df = df.withColumn('ranking',rank().over(window_spec))

window_df.filter(col('ranking')==1).select('Region','Month','Sales').show()
