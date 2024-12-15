from pyspark.sql.functions import col, lit, monotonically_increasing_id
from src.base.spark_task import SparkTask

class FactUserActivityTransformerTask(SparkTask):
    def __init__(self, spark, deposits, withdrawals, events):
        super().__init__(spark)
        self.deposits = deposits
        self.withdrawals = withdrawals
        self.events = events

    def run(self):
        self.logger.info("Transforming FactUserActivity table.")
        fact_user_activity = self.deposits.withColumn("activity_type", lit("deposit")) \
            .select("user_id", "event_timestamp", "amount", "currency", "activity_type") \
            .union(
                self.withdrawals.withColumn("activity_type", lit("withdrawal"))
                .select("user_id", "event_timestamp", "amount", "currency", "activity_type")
            ).union(
                self.events.withColumn("activity_type", lit("login"))
                .select("user_id", "event_timestamp", lit(None).alias("amount"), lit(None).alias("currency"), "activity_type")
            ).withColumn("currency_id", monotonically_increasing_id())
        return fact_user_activity