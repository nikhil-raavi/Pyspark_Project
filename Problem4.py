'''

Q)For each user, calculate the average gap in days between consecutive transactions.
Identify the user with the largest average gap.
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('Pyspark Practice').config('key','value').getOrCreate()

data = [
 (1, 101, 500.0, "2024-01-01"),
 (2, 102, 200.0, "2024-01-02"),
 (3, 101, 300.0, "2024-01-03"),
 (4, 103, 100.0, "2024-01-04"),
 (5, 102, 400.0, "2024-01-05"),
 (6, 103, 600.0, "2024-01-06"),
 (7, 101, 200.0, "2024-01-07"),
]
columns = ["transaction_id", "user_id", "transaction_amount", "transaction_date"]

df = spark.createDataFrame(data=data,schema=columns)
df = df.withColumn('transaction_date',to_date(col('transaction_date'),'yyyy-mm-dd'))

# df.show()
window_spec = Window.partitionBy('user_id').orderBy(desc('transaction_date'))
df = df.withColumn('previous_date',lag("transaction_date").over(window_spec))
df = df.withColumn("day_gap",date_diff(col('transaction_date'),col('previous_date')))
df.show()
avg_gap_df = df.groupby(col('user_id')).agg(avg(col('day_gap')).alias('average_gap'))
avg_gap_df.show()



