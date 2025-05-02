'''
𝐐𝐮𝐞𝐬𝐭𝐢𝐨𝐧
Imagine you're analyzing the monthly sales performance of a company across different regions. You want to calculate:
The cumulative sales for each region over months.
The rank of each month based on sales within the same region.
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('Pyspark Practice').config('key','value').getOrCreate()

data = [ ("East", "Jan", 200), ("East", "Feb", 300),
("East", "Mar", 250), ("West", "Jan", 400),
("West", "Feb", 350), ("West", "Mar", 450) ]

columns_schema = StructType([ \
 StructField("Region",StringType(),True),
 StructField("Month",StringType(),True), \
 StructField("Sales",IntegerType(),True)])

df = spark.createDataFrame(data=data,schema= columns_schema)

df.show()

window_spec = Window.partitionBy(['Region']).orderBy(desc('Sales'))

window_df = df.withColumn('Cummulative_Sales',sum('Sales').over(window_spec))\
            .withColumn('Ranking',dense_rank().over(window_spec))

window_df.show()