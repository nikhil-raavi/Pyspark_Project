from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('Pyspark Practice').config('key','value').getOrCreate()

data = [ (1, "Alice", None), (2, "Bob", 1),
(3, "Charlie", 1), (4, "David", 2),
(5, "Eva", 2), (6, "Frank", 3), (7, "Grace", 3) ]

columns = ["employee_id", "employee_name", "manager_id"]

df = spark.createDataFrame(data,columns)

df2 = df.alias('a').join(df.alias('b'), col('a.manager_id') == col('b.employee_id'),'left').\
    select(col('b.employee_name').alias('Manager'),col('a.employee_name').alias('Employee'))


df2.show()