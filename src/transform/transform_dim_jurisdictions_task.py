from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src.base.spark_task import SparkTask

class DimJurisdictionsTransformerTask(SparkTask):
    def __init__(self, spark):
        super().__init__(spark)

    def run(self):
        self.logger.info("Transforming DimJurisdictions table.")
        dim_jurisdictions = self.spark.createDataFrame([
            (1, "MX", "Mexico", "MXN", "Mexican regulations"),
            (2, "US", "United States", "USD", "US regulations")
        ], schema=StructType([
            StructField("jurisdiction_id", IntegerType(), False),
            StructField("country_code", StringType(), False),
            StructField("jurisdiction_name", StringType(), False),
            StructField("currency", StringType(), False),
            StructField("regulatory_info", StringType(), False),
        ]))
        return dim_jurisdictions