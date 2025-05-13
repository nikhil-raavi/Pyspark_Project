
# Write a pyspark program to find all dates with high temperatures compared to their previous dates(yesterday).
from datetime import time
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *

# Create a SparkSession
spark = SparkSession.builder.appName("Temprature calculate").getOrCreate()
# Define the data
data = [
 (1, '2025-01-01', 10),
 (2, '2025-01-02', 12),
 (3, '2025-01-03', 13),
 (4, '2025-01-04', 11),
 (5, '2025-01-05', 12),
 (6, '2025-01-06', 9),
 (7, '2025-01-07', 10),
]
columns=['id', 'date', 'temperature']
# Create the DataFrame
df = spark.createDataFrame(data, columns)

window_spec = Window.orderBy('date')
windowed_df = (df.withColumn('previous_temperature',lag('temperature').over(window_spec)).\
               filter(col('temperature') > col('previous_temperature')))

windowed_df.show()