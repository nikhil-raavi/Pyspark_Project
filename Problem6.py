


from pyspark.sql.functions import*
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('Pyspark Practice').config('key','value').getOrCreate()
# Calculate the running total

'''
data = [ ("2024-09-01", "AAPL", 150), ("2024-09-02", "AAPL", 160),
("2024-09-03", "AAPL", 170), ("2024-09-01", "GOOGL", 1200),
 ("2024-09-02", "GOOGL", 1250), ("2024-09-03", "GOOGL", 1300) ]

# Create DataFrame
df = spark.createDataFrame(data, ["date", "symbol", "price"])

window_spec = Window.partitionBy('symbol').orderBy('date').rowsBetween(Window.unboundedPreceding,
                                                                       Window.currentRow)
df = df.withColumn('running_total',sum('price').over(window_spec))
df.show()

'''

# 𝐐𝐮𝐞𝐬𝐭𝐢𝐨𝐧 22
# To calculate the percentage of total salary that each employee contributes to their respective department.

# Sample Data
data = [
    (1, 'Alice', 'HR', 5000),
    (2, 'Bob', 'HR', 7000),
    (3, 'Charlie', 'IT', 10000),
    (4, 'David', 'IT', 8000),
    (5, 'Eve', 'IT', 6000)
]

# Create DataFrame (fixed syntax)
columns = ["Employee_ID", "Name", "Department", "Salary"]
df = spark.createDataFrame(data, columns)  # Fixed: "," instead of ";"

df_total_salary = df.groupby('Department').agg(sum('Salary').alias('Total_Salary'))
df_with_total = df.join(df_total_salary, on="Department")

df_final = df_with_total.withColumn('percentage_contribution',round(col('Salary')/col('Total_Salary')*100,2))

# df_total_salary.show()
# df_with_total.show()

df_final.show()