'''❓Task:
For each customer, return the latest order amount, the total amount spent,
and the number of orders. Output should include the customer's name.
'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('Pyspark Practice').config('key','value').getOrCreate()

# Sample data

orders_data = [
(1, 101, '2024-01-01', 500),
(2, 102, '2024-01-03', 300), (3, 101, '2024-02-01', 700),
(4, 103, '2024-01-10', 100),
(5, 101, '2024-03-15', 200),
]
customers_data = [
(101, 'Alice'),
(102, 'Bob'),
(103, 'Charlie'),
]

#Create DataFrames

orders_df = spark.createDataFrame(orders_data, ['order_id', 'customer_id', 'order_date', 'amount'])
customers_df = spark.createDataFrame(customers_data, ['customer_id', 'name'])
# orders_df.show()
# customers_df.show()

window_rank = Window.partitionBy('customer_id').orderBy(desc('order_date'))
window_total = Window.partitionBy('customer_id')

updated_df = orders_df.withColumn('ranking',dense_rank().over(window_rank)).\
            withColumn('total_amount_spent',sum('amount').over(window_total)).\
            withColumn('no_of_orders',count('customer_id').over(window_total))\
            .filter(col("ranking") == 1)


updated_df.show()

output_df = updated_df.join(customers_df,'customer_id','left').\
          select("name", "amount", "total_amount_spent", "no_of_orders")
output_df.show()
