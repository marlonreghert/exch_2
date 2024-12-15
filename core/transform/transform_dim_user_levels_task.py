from pyspark.sql.functions import col
from core.base.spark_task import SparkTask

class DimUserLevelsTransformerTask(SparkTask):
    def __init__(self, spark, user_levels):
        super().__init__(spark)
        self.user_levels = user_levels

    def run(self):
        self.logger.info("Transforming DimUserLevels table.")
        dim_user_levels = self.user_levels.select(
            col("user_id"),
            col("jurisdiction"),
            col("level"),
            col("event_timestamp").alias("effective_date")
        ).dropDuplicates()
        return dim_user_levels
