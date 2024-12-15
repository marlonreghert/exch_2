from pyspark.sql.functions import col, monotonically_increasing_id
from core.base.spark_task import SparkTask

class FactWithdrawalsTransformerTask(SparkTask):
    def __init__(self, spark, withdrawals):
        super().__init__(spark)
        self.withdrawals = withdrawals

    def run(self):
        self.logger.info("Transforming FactWithdrawals table.")
        fact_withdrawals = self.withdrawals.filter(col("tx_status") == "complete") \
            .select(
                col("user_id"),
                col("event_timestamp").alias("withdrawal_date"),
                col("amount"),
                col("currency")
            ).withColumn("currency_id", monotonically_increasing_id())
        return fact_withdrawals