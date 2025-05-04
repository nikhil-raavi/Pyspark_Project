'''
Write code to add a new column, Country, to the DataFrame using different methods
in PySpark. The Country column values should be based on the following mapping:
New York: USA
London: UK
Sydney: Australia
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('Pyspark Practice').config('key','value').getOrCreate()

data = [("John", 28, "New York"), ("Sarah", 24, "London"), ("Michael", 30, "Sydney")]
columns = ["Name", "Age", "City"]

# Create DataFrame
df = spark.createDataFrame(data, columns)
'''
df = df.withColumn('Country',expr("CASE WHEN City = 'New York' THEN 'USA'\
                                                WHEN City = 'London' THEN 'UK' \
                                               When City = 'Sydney' THEN 'Australia'\
                                                END"))
'''
df = df.withColumn('Country',when(col('City') == 'New York','USA')
                                      .when(col('City') == 'London','UK')
                                  .otherwise('Australia'))


df.show()