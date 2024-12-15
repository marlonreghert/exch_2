from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from core.base.spark_task import SparkTask

class DimProductsTransformerTask(SparkTask):
    def __init__(self, spark):
        super().__init__(spark)

    def run(self):
        self.logger.info("Transforming DimProducts table.")
        dim_products = self.spark.createDataFrame([
            (1, "Staking", "Financial", "MX,US", "2023-01-01"),
            (2, "Lending", "Financial", "MX", "2022-06-01"),
            (3, "Rewards", "Reward", "US", "2021-05-01")
        ], schema=StructType([
            StructField("product_id", IntegerType(), False),
            StructField("product_name", StringType(), False),
            StructField("product_type", StringType(), False),
            StructField("availability", StringType(), False),
            StructField("launch_date", StringType(), False),
        ]))
        return dim_products