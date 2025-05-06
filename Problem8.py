from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('Pyspark Practice').config('key','value').getOrCreate()

# data = [ (1, "Alice", None), (2, "Bob", 1),
# (3, "Charlie", 1), (4, "David", 2),
# (5, "Eva", 2), (6, "Frank", 3), (7, "Grace", 3) ]
#
# columns = ["employee_id", "employee_name", "manager_id"]
#
# df = spark.createDataFrame(data,columns)
#
# df2 = df.alias('a').join(df.alias('b'), col('a.manager_id') == col('b.employee_id'),'left').\
#     select(col('b.employee_name').alias('Manager'),col('a.employee_name').alias('Employee'))
#
#
# df2.show()

data = [
    (1, 101, "2024-04-10", 200),
    (2, 101, "2024-04-12", 300),
    (3, 101, "2024-04-13", 250),
    (4, 102, "2024-04-10", 100),
    (5, 102, "2024-04-11", 200),
    (6, 102, "2024-04-12", 300),
]

# Schema definition
schema = StructType([
    StructField("txn_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("txn_date", StringType(), False),
    StructField("amount", IntegerType(), False)
])



# Create DataFrame
df_transactions = spark.createDataFrame(data, schema)
df_transactions.show()


window_spec = Window.partitionBy('customer_id').orderBy(col('txn_date'))
window_df = df_transactions.withColumn('next_transaction',lead('amount').over(window_spec)).\
                                        filter(col('amount') < col('next_transaction'))
result_df = window_df.select("customer_id").distinct()

window_df.show()
result_df.show()