import logging
from pyspark.sql import SparkSession
from src.read.read_tables_task import ReadTablesTask
from src.transform.transform_dim_date_task import DimDateTransformerTask
from src.transform.transform_dim_users_task import DimUsersTransformerTask
from src.transform.transform_dim_user_levels_task import DimUserLevelsTransformerTask
from src.transform.transform_dim_products_task import DimProductsTransformerTask
from src.transform.transform_dim_jurisdictions_task import DimJurisdictionsTransformerTask
from src.transform.transform_fact_deposits_task import FactDepositsTransformerTask
from src.transform.transform_fact_withdrawals_task import FactWithdrawalsTransformerTask
from src.transform.transform_fact_user_activity_task import FactUserActivityTransformerTask
from src.write.save_table_task import SaveTableTask
from datetime import datetime
from time import sleep
import uuid

# Configuring the ETL input size
# full => the full input (100% of data)
# sample => a sample of data (~1% of data)
INPUT_TYPE = "full" 

# Output directory
LOGICAL_DATE = datetime.now()
LOGICAL_DAILY_PATH = f"{LOGICAL_DATE.year}/{LOGICAL_DATE.month}/{LOGICAL_DATE.day}"
OUTPUT_PATH = f"./output/{LOGICAL_DAILY_PATH}"

# Define file paths for raw input data
FILE_PATHS = {
    "deposits": f"./resources/input/{INPUT_TYPE}/{LOGICAL_DAILY_PATH}/deposit_sample_data.csv",
    "withdrawals": f"./resources/input/{INPUT_TYPE}/{LOGICAL_DAILY_PATH}/withdrawals_sample_data.csv",
    "users": f"./resources/input/{INPUT_TYPE}/{LOGICAL_DAILY_PATH}/user_id_sample_data.csv",
    "user_levels": f"./resources/input/{INPUT_TYPE}/{LOGICAL_DAILY_PATH}/user_level_sample_data.csv",
    "events": f"./resources/input/{INPUT_TYPE}/{LOGICAL_DAILY_PATH}/event_sample_data.csv",
}

# ETL interval (1 day)
RUN_SCHEDULE_SECONDS = 86400

class BitsoDataPipeline:
    def __init__(self):
        self.logger = logging.getLogger("BitsoDataPipeline")

    def run(self):
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("BitsoDataPipeline") \
            .getOrCreate()
        
        self.logger.info("Pipeline started.")

        # Step 1: Load raw data
        load_task = ReadTablesTask(spark, FILE_PATHS)
        deposits, withdrawals, users, user_levels, events = load_task.run()
        self.logger.info("Data loading complete.")

        # Step 2: Transform dimension tables
        dim_date_task = DimDateTransformerTask(spark)
        dim_date = dim_date_task.run()
        self.logger.info("DimDate transformation complete.")

        dim_users_task = DimUsersTransformerTask(spark, users)
        dim_users = dim_users_task.run()
        self.logger.info("DimUsers transformation complete.")

        dim_user_levels_task = DimUserLevelsTransformerTask(spark, user_levels)
        dim_user_levels = dim_user_levels_task.run()
        self.logger.info("DimUserLevels transformation complete.")

        dim_products_task = DimProductsTransformerTask(spark)
        dim_products = dim_products_task.run()
        self.logger.info("DimProducts transformation complete.")

        dim_jurisdictions_task = DimJurisdictionsTransformerTask(spark)
        dim_jurisdictions = dim_jurisdictions_task.run()
        self.logger.info("DimJurisdictions transformation complete.")

        # Step 3: Transform fact tables
        fact_deposits_task = FactDepositsTransformerTask(spark, deposits)
        fact_deposits = fact_deposits_task.run()
        self.logger.info("FactDeposits transformation complete.")

        fact_withdrawals_task = FactWithdrawalsTransformerTask(spark, withdrawals)
        fact_withdrawals = fact_withdrawals_task.run()
        self.logger.info("FactWithdrawals transformation complete.")

        fact_user_activity_task = FactUserActivityTransformerTask(spark, deposits, withdrawals, events)
        fact_user_activity = fact_user_activity_task.run()
        self.logger.info("FactUserActivity transformation complete.")

        # Step 4: Save all tables
        save_dim_date_task = SaveTableTask(spark, dim_date, OUTPUT_PATH, "DimDate", format="csv")
        save_dim_date_task.run()
        self.logger.info("DimDate saved.")

        save_dim_users_task = SaveTableTask(spark, dim_users, OUTPUT_PATH, "DimUsers", format="csv")
        save_dim_users_task.run()
        self.logger.info("DimUsers saved.")

        save_dim_user_levels_task = SaveTableTask(spark, dim_user_levels, OUTPUT_PATH, "DimUserLevels", format="csv")
        save_dim_user_levels_task.run()
        self.logger.info("DimUserLevels saved.")

        save_dim_products_task = SaveTableTask(spark, dim_products, OUTPUT_PATH, "DimProducts", format="csv")
        save_dim_products_task.run()
        self.logger.info("DimProducts saved.")

        save_dim_jurisdictions_task = SaveTableTask(spark, dim_jurisdictions, OUTPUT_PATH, "DimJurisdictions", format="csv")
        save_dim_jurisdictions_task.run()
        self.logger.info("DimJurisdictions saved.")

        save_fact_deposits_task = SaveTableTask(spark, fact_deposits, OUTPUT_PATH, "FactDeposits", format="csv")
        save_fact_deposits_task.run()
        self.logger.info("FactDeposits saved.")

        save_fact_withdrawals_task = SaveTableTask(spark, fact_withdrawals, OUTPUT_PATH, "FactWithdrawals", format="csv")
        save_fact_withdrawals_task.run()
        self.logger.info("FactWithdrawals saved.")

        save_fact_user_activity_task = SaveTableTask(spark, fact_user_activity, OUTPUT_PATH, "FactUserActivity", format="csv")
        save_fact_user_activity_task.run()
        self.logger.info("FactUserActivity saved.")

        self.logger.info("Pipeline completed successfully.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("PipelineOrchestrator")

    dataPipeline = BitsoDataPipeline()
    while True:
        try:
            logger.info("Running bitso data pipeline")
            dataPipeline.run()
            logger.info("Pipeline completed successfully.")
        except Exception as e:
            logger.exception("Exception occurred while running BitsoDataPipeline")
        finally:
            logger.info(f"Waiting {RUN_SCHEDULE_SECONDS} seconds until the next execution.")
            sleep(RUN_SCHEDULE_SECONDS)
