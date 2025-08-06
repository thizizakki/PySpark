import os
import sys
from pyspark.sql import SparkSession

def main():
    # Decide mode: local or cluster
    mode = sys.argv[1] if len(sys.argv) > 1 else "local"

    spark = SparkSession.builder.appName("WordCount").master("local[*]" if mode == "local" else "yarn").getOrCreate()

    sc = spark.sparkContext

    sc._jsc.hadoopConfiguration().set("hadoop.native.lib", "false")
    # Input paths
    input_file = ("D:\\PySpark\\Data\\testData1.log")

    # Output path (local)
    output_path = "D:/PySpark/output_wordcount"

    # Delete old output folder to prevent Spark errors
    if os.path.exists(output_path):
        import shutil
        shutil.rmtree(output_path)

    # Word count
    wordcount = (
        sc.textFile(input_file)
        .flatMap(lambda x: x.split())
        .map(lambda x: (x, 1))
        .reduceByKey(lambda a, b: a + b)
        .sortBy(lambda x: x[0])
    )

    # Save to local system
    wordcount.saveAsTextFile(output_path)

    print(f"Results saved to: {output_path}")

    spark.stop()

if __name__ == "__main__":
    main()
