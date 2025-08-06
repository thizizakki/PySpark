
from pyspark.sql import *
import os

# Set Python executable for both driver and workers
os.environ["PYSPARK_PYTHON"] = r"D:\PySpark\.venv\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"D:\PySpark\.venv\Scripts\python.exe"

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("WordCount").getOrCreate()
    sc = spark.sparkContext
    lines = sc.textFile("D:\\PySpark\\Data\\Input.log")
    # wordcount = lines.flatMap(lambda x: x.split()).map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b).sortBy(
    #     lambda x: x[0])

    filtered = lines.filter(lambda line: "Spark" in line)

    for word in filtered.collect():
        print(word)

    sc.stop()
