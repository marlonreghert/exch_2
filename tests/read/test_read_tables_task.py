import unittest
from unittest.mock import MagicMock
import uuid
import logging
from src.base.spark_task import SparkTask
from src.read.read_tables_task import ReadTablesTask
from pyspark.sql import SparkSession

class TestReadTablesTask(unittest.TestCase):
    def setUp(self):
        self.mock_spark = MagicMock(spec=SparkSession)
        self.mock_file_paths = {
            "deposits": "path/to/deposits.csv",
            "withdrawals": "path/to/withdrawals.csv",
            "users": "path/to/users.csv",
            "user_levels": "path/to/user_levels.csv",
            "events": "path/to/events.csv",
        }

    def test_initialization(self):
        task = ReadTablesTask(self.mock_spark, self.mock_file_paths)
        self.assertEqual(task.file_paths, self.mock_file_paths)
        self.assertEqual(task.spark, self.mock_spark)

    def test_run_calls_spark_read(self):
        task = ReadTablesTask(self.mock_spark, self.mock_file_paths)
        task.run()
        self.mock_spark.read.csv.assert_any_call("path/to/deposits.csv", header=True, inferSchema=True)
        self.mock_spark.read.csv.assert_any_call("path/to/withdrawals.csv", header=True, inferSchema=True)
        self.mock_spark.read.csv.assert_any_call("path/to/users.csv", header=True, inferSchema=True)
        self.mock_spark.read.csv.assert_any_call("path/to/user_levels.csv", header=True, inferSchema=True)
        self.mock_spark.read.csv.assert_any_call("path/to/events.csv", header=True, inferSchema=True)

    def test_run_returns_expected_output(self):
        self.mock_spark.read.csv.return_value = MagicMock()
        task = ReadTablesTask(self.mock_spark, self.mock_file_paths)
        result = task.run()
        self.assertEqual(len(result), 5)

if __name__ == "__main__":
    unittest.main()
