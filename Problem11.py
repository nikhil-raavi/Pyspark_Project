from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('Pyspark Practice').config('key','value').getOrCreate()
'''
Task: Write PySpark code to transform this DataFrame so that each tag appears in its own row,
along with the corresponding id, using split() and explode().
'''
'''
data=[(1,'spark,hadoop,hive'),(2,'python,flask'),(3,'sql')]
df = spark.createDataFrame(data,['id','tags'])

df = df.withColumn('tag',explode(col('tags')))

df.select('id','tag').show()

# df.show()

'''
'''
𝐐𝐮𝐞𝐬𝐭𝐢𝐨𝐧
You are given an employee dataset containing information about employees and their managers. 
Each employee has a manager_id that refers to another employee in the same table. 
Your task is to use self-join to find hierarchical relationships between employees, 
such as finding all employees under a specific manager or the reporting hierarchy of an employee.

Interview Task
Write a PySpark self-join query to find the direct reports of each manager. 
Additionally, extend the logic to find all hierarchical relationships up to any level.
'''

data = [ (1, "Alice", None), (2, "Bob", 1),
(3, "Charlie", 1), (4, "David", 2),
(5, "Eva", 2), (6, "Frank", 3), (7, "Grace", 3) ]

columns = ["employee_id", "employee_name", "manager_id"]

df = spark.createDataFrame(data,columns)
join_df = df.alias('a').join(df.alias('b'),col('a.manager_id')==col('b.employee_id'),'left')
join_df.show()