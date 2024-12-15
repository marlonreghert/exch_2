from pyspark.sql.functions import col, monotonically_increasing_id
from core.base.spark_task import SparkTask

class FactDepositsTransformerTask(SparkTask):
    def __init__(self, spark, deposits):
        super().__init__(spark)
        self.deposits = deposits

    def run(self):
        self.logger.info("Transforming FactDeposits table.")
        fact_deposits = self.deposits.filter(col("tx_status") == "complete") \
            .select(
                col("user_id"),
                col("event_timestamp").alias("deposit_date"),
                col("amount"),
                col("currency")
            ).withColumn("currency_id", monotonically_increasing_id())
        return fact_deposits