from pyspark.sql import SparkSession

class SparkSessionManager:
    def __init__(self, app_name, master_config):
        self.spark = SparkSession \
            .builder \
            .appName(app_name) \
            .master(master_config) \
            .getOrCreate()

    def get_spark_session(self):
        return self.spark
