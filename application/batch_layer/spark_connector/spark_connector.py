from pyspark import SparkContext
from pyspark.sql import SparkSession

# Initialize Spark
sc = SparkContext(appName="BatchLayer")
spark = SparkSession(sc)

sc.stop()

class SparkConnector:
    def __init__(self, app_name):
        self.spark = SparkSession.builder.appName(app_name).getOrCreate()

    def batch_process(self, csv: str) -> :
        batch_data = spark.read.csv(csv, header=True)
        # Perform batch processing (replace with your batch processing logic)
        batch_result = batch_data.groupBy("user").count()
        # Save batch result to a file (replace with your storage solution)
        batch_result.write.csv("batch_result.csv", header=True)