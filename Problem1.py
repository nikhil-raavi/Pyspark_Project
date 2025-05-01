# Quoestion
# Which item was purchased just before the customer became a member?
# P.S. We now deal with 3 tables
#
#
# 𝐒𝐡𝐨𝐫𝐭 𝐞𝐱𝐩𝐥𝐚𝐧𝐚𝐭𝐢𝐨𝐧:
# Find the last 𝐢𝐭𝐞𝐦 𝐚 𝐜𝐮𝐬𝐭𝐨𝐦𝐞𝐫 𝐛𝐨𝐮𝐠𝐡𝐭 𝐫𝐢𝐠𝐡𝐭 𝐛𝐞𝐟𝐨𝐫𝐞 they officially became a member —
# the final purchase that happened before their join date.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('Pyspark Practice').config('key','value').getOrCreate()

sales_data = [
 ('A', '2021-01-01', '1'),('A', '2021-01-01', '2'),('A', '2021-01-07', '2'),
 ('A', '2021-01-10', '3'),('A', '2021-01-11', '3'),('A', '2021-01-11', '3'),
 ('B', '2021-01-01', '2'),('B', '2021-01-02', '2'),('B', '2021-01-04', '1'),
 ('B', '2021-01-11', '1'),('B', '2021-01-16', '3'),('B', '2021-02-01', '3'),
 ('C', '2021-01-01', '3'),('C', '2021-01-01', '3'),('C', '2021-01-07', '3')]

sales_schema = StructType([ \
 StructField("customer_id",StringType(),True),
 StructField("order_date",StringType(),True), \
 StructField("product_id",StringType(),True)])

menu_data = [('1', 'sushi', 10),('2', 'curry', 15),('3', 'ramen', 12)]

menu_schema = StructType([ \
 StructField("product_id",StringType(),True),\
 StructField("product_name",StringType(),True),\
 StructField("price",IntegerType(),True)])

members_schema = StructType([
StructField("customer_id",StringType(),True),
StructField("join_date",StringType(),True)])

members_data = [('A', '2021-01-07'),('B', '2021-01-09')]


sales_df = spark.createDataFrame(data=sales_data,schema= sales_schema)
menu_df = spark.createDataFrame(data=menu_data,schema= menu_schema)
members_df = spark.createDataFrame(data=members_data,schema= members_schema)

sales_df.show()
menu_df.show()
members_df.show()

