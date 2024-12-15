import unittest
from unittest.mock import MagicMock,  patch
import uuid
import logging
from src.transform.transform_dim_date_task import DimDateTransformerTask
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date, date_add, year, month, dayofmonth, dayofweek


class TestDimDateTransformerTask(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .master("local[1]") \
            .appName("TestDimDateTransformerTask") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_run_creates_dim_date(self):
        task = DimDateTransformerTask(self.spark)
        result = task.run()
        self.assertTrue("date_id" in result.columns)
        self.assertTrue("calendar_date" in result.columns)
        self.assertTrue("year" in result.columns)
        self.assertTrue("month" in result.columns)
        self.assertTrue("day" in result.columns)
        self.assertTrue("weekday" in result.columns)

    def test_run_returns_dataframe(self):
        task = DimDateTransformerTask(self.spark)
        result = task.run()
        self.assertEqual(result.count(), 1999)  # Expecting 1999 rows as range(1, 2000)
        self.assertTrue(result.select("date_id").count() > 0)


if __name__ == "__main__":
    unittest.main()