from pyspark.sql import SparkSession
from core.base.spark_task import SparkTask

class ReadTablesTask(SparkTask):
    def __init__(self, spark, file_paths):
        super().__init__(spark)
        self.file_paths = file_paths

    def run(self):
        self.logger.info("Loading raw data.")
        
        deposits = self.spark.read.csv(self.file_paths["deposits"], header=True, inferSchema=True)
        withdrawals = self.spark.read.csv(self.file_paths["withdrawals"], header=True, inferSchema=True)
        users = self.spark.read.csv(self.file_paths["users"], header=True, inferSchema=True)
        user_levels = self.spark.read.csv(self.file_paths["user_levels"], header=True, inferSchema=True)
        events = self.spark.read.csv(self.file_paths["events"], header=True, inferSchema=True)
        
        return deposits, withdrawals, users, user_levels, events
