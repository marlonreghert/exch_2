import logging
import uuid
from pyspark.sql import DataFrame
from core.base.spark_task import SparkTask

class SaveTableTask(SparkTask):
    def __init__(self, spark, table, output_path, table_name, format="csv", coalesce=True):
        """
        Initialize the SaveTableTask.
        :param spark: Spark session object.
        :param table: DataFrame to save.
        :param output_path: Base directory where the table will be saved.
        :param table_name: Name of the table to save.
        :param format: Format in which to save the table (default is "csv").
        :param coalesce: Whether to coalesce the DataFrame to a single file (default is True).
        """
        super().__init__(spark)
        self.table = table
        self.output_path = output_path
        self.table_name = table_name
        self.format = format
        self.coalesce = coalesce

    def run(self):
        """
        Save the DataFrame to the specified path in the specified format.
        """
        self.logger.info(f"Saving table: {self.table_name} in {self.format} format.")
        output_dir = f"{self.output_path}/{self.table_name}"
        
        try:
            if self.coalesce:
                self.table = self.table.coalesce(1)
            
            if self.format == "csv":
                self.table.write.csv(output_dir, header=True, mode="overwrite")
            elif self.format == "parquet":
                self.table.write.parquet(output_dir, mode="overwrite")
            elif self.format == "json":
                self.table.write.json(output_dir, mode="overwrite")
            else:
                raise ValueError(f"Unsupported format: {self.format}")
            
            self.logger.info(f"Successfully saved table: {self.table_name} to {output_dir}.")
        except Exception as e:
            self.logger.error(f"Error saving table: {self.table_name}. Details: {str(e)}")
            raise e
