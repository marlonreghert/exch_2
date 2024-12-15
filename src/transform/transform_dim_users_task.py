from src.base.spark_task import SparkTask

class DimUsersTransformerTask(SparkTask):
    def __init__(self, spark, users):
        super().__init__(spark)
        self.users = users

    def run(self):
        self.logger.info("Transforming DimUsers table.")
        return self.users.select("user_id").dropDuplicates()
