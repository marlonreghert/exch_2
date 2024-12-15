import unittest
from unittest.mock import MagicMock
import uuid
import logging
from src.base.spark_task import SparkTask


class TestSparkTask(unittest.TestCase):
    def setUp(self):
        self.mock_spark = MagicMock() 

    def test_task_id_is_unique(self):
        task1 = SparkTask(self.mock_spark)
        task2 = SparkTask(self.mock_spark)
        self.assertNotEqual(task1.get_task_id(), task2.get_task_id(), "Task IDs should be unique.")

    def test_logger_is_initialized(self):
        task = SparkTask(self.mock_spark)
        self.assertIsInstance(task.logger, logging.Logger, "Logger should be an instance of logging.Logger.")
        self.assertEqual(task.logger.name, "SparkTask", "Logger name should match class name.")

    def test_spark_is_assigned(self):
        task = SparkTask(self.mock_spark)
        self.assertEqual(task.spark, self.mock_spark, "Spark instance should be assigned correctly.")

    def test_get_task_id(self):
        task = SparkTask(self.mock_spark)
        task_id = task.get_task_id()
        self.assertTrue(isinstance(task_id, str), "Task ID should be a string.")
        self.assertTrue(len(task_id) > 0, "Task ID should not be empty.")

    def test_run_not_implemented(self):
        task = SparkTask(self.mock_spark)
        with self.assertRaises(NotImplementedError):
            task.run()

if __name__ == "__main__":
    unittest.main()
