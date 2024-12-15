from pyspark.sql.functions import col, lit, to_date, date_add, year, month, dayofmonth, dayofweek
from src.base.spark_task import SparkTask

class DimDateTransformerTask(SparkTask):
    def __init__(self, spark):
        super().__init__(spark)

    def run(self):
        self.logger.info("Transforming DimDate table.")
        date_range = self.spark.range(1, 2000).withColumn("id", col("id").cast("int"))
        date_range = date_range.withColumn("calendar_date", date_add(to_date(lit("2019-01-01")), col("id")))
        
        dim_date = date_range.select(
            col("id").alias("date_id"),
            col("calendar_date"),
            year("calendar_date").alias("year"),
            month("calendar_date").alias("month"),
            dayofmonth("calendar_date").alias("day"),
            dayofweek("calendar_date").alias("weekday")
        )
        return dim_date
