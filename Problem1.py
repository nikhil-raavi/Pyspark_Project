# Quoestion
# Which item was purchased just before the customer became a member?
# P.S. We now deal with 3 tables

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
# sales_df.show()
# menu_df.show()
# members_df.show()
df_joined = sales_df.alias('a').join(
    members_df.alias('b'),
    (col('a.customer_id') == col('b.customer_id')) &
    (col('a.order_date') < col('b.join_date')),
    "inner"
).drop(col("b.customer_id"))  # Drop redundant column
# df_joined.show()

window_spec = Window.partitionBy("customer_id").orderBy(desc("order_date"))
windowed_df = df_joined.withColumn('ranking', dense_rank().over(window_spec)).filter(col('ranking') == 1)
# windowed_df.show()

output_df = windowed_df.alias('c').join(
    menu_df.alias('d'),
    col('c.product_id') == col('d.product_id'),
    'inner'
).select(
    col('c.customer_id'),
    col('d.product_name').alias('dish before becoming customer')
)

output_df.show()

# 𝐐𝐮𝐞𝐬𝐭𝐢𝐨𝐧 𝐨𝐟 𝐭𝐡𝐞 𝐃𝐚𝐲!
# 𝐖𝐡𝐚𝐭 𝐢𝐬 𝐭𝐡𝐞 𝐭𝐨𝐭𝐚𝐥 𝐚𝐦𝐨𝐮𝐧𝐭 𝐞𝐚𝐜𝐡 𝐜𝐮𝐬𝐭𝐨𝐦𝐞𝐫 𝐬𝐩𝐞𝐧𝐭 𝐚𝐭 𝐭𝐡𝐞 𝐫𝐞𝐬𝐭𝐚𝐮𝐫𝐚𝐧𝐭?

output_df = sales_df.join(menu_df,'product_id','inner').\
            groupby('customer_id').agg(sum('price').alias("total_mount_spent")).orderBy('total_mount_spent')

output_df.show()