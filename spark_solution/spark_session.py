# Path: spark_solution/spark_session.py

from pyspark.sql import SparkSession


class SparkContextManager:

    def __init__(self, app_name="CPS Data Analysis"):
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
            .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
            .getOrCreate()

    def get_spark_session(self):
        return self.spark

    def stop_spark_session(self):
        if self.spark:
            self.spark.stop()
            print("Spark session stopped.")
