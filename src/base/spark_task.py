import uuid
import logging

class SparkTask:
    def __init__(self, spark):
        self.task_id = str(uuid.uuid4())  # Generate a unique ID
        self.logger = logging.getLogger(self.__class__.__name__)
        self.spark = spark
        
    def get_task_id(self):
        return self.task_id

    def run(self):
        raise NotImplementedError("Subclasses must implement the `run` method.")            