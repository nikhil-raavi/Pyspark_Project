
# Calculate the running total


from pyspark.sql.functions import*
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('Pyspark Practice').config('key','value').getOrCreate()

data = [ ("2024-09-01", "AAPL", 150), ("2024-09-02", "AAPL", 160),
("2024-09-03", "AAPL", 170), ("2024-09-01", "GOOGL", 1200),
 ("2024-09-02", "GOOGL", 1250), ("2024-09-03", "GOOGL", 1300) ]

# Create DataFrame
df = spark.createDataFrame(data, ["date", "symbol", "price"])

window_spec = Window.partitionBy('symbol').orderBy('date').rowsBetween(Window.unboundedPreceding,
                                                                       Window.currentRow)
df = df.withColumn('running_total',sum('price').over(window_spec))
df.show()