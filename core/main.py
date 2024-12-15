import logging
from pyspark.sql import SparkSession
from core.read.read_tables_task import ReadTablesTask
from core.transform.transform_dim_date_task import DimDateTransformerTask
from core.transform.transform_dim_users_task import DimUsersTransformerTask
from core.transform.transform_dim_user_levels_task import DimUserLevelsTransformerTask
from core.transform.transform_dim_products_task import DimProductsTransformerTask
from core.transform.transform_dim_jurisdictions_task import DimJurisdictionsTransformerTask
from core.transform.transform_fact_deposits_task import FactDepositsTransformerTask
from core.transform.transform_fact_withdrawals_task import FactWithdrawalsTransformerTask
from core.transform.transform_fact_user_activity_task import FactUserActivityTransformerTask
# from core.transform.transform_fact_staking_task import FactStakingTransformerTask
from core.write.save_table_task import SaveTableTask
from datetime import datetime
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("PipelineOrchestrator")

# Configuring the ETL input size
# full => the full input (100% of data)
# sample => a sample of data (~1% of data)
INPUT_TYPE = "full" 

# Define file paths for raw input data
file_paths = {
    "deposits": f"./resources/input/{INPUT_TYPE}/deposit_sample_data.csv",
    "withdrawals": f"./resources/input/{INPUT_TYPE}/withdrawals_sample_data.csv",
    "users": f"./resources/input/{INPUT_TYPE}/user_id_sample_data.csv",
    "user_levels": f"./resources/input/{INPUT_TYPE}/user_level_sample_data.csv",
    "events": f"./resources/input/{INPUT_TYPE}/event_sample_data.csv",
}

# Output directory
LOGICAL_DATE = datetime.now()
OUTPUT_PATH = f"./output/{LOGICAL_DATE.year}/{LOGICAL_DATE.month}/{LOGICAL_DATE.day}"

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("BitsoDataPipeline") \
        .getOrCreate()
    
    logger.info("Pipeline started.")

    # Step 1: Load raw data
    load_task = ReadTablesTask(spark, file_paths)
    deposits, withdrawals, users, user_levels, events = load_task.run()
    logger.info("Data loading complete.")

    # Step 2: Transform dimension tables
    dim_date_task = DimDateTransformerTask(spark)
    dim_date = dim_date_task.run()
    logger.info("DimDate transformation complete.")

    dim_users_task = DimUsersTransformerTask(spark, users)
    dim_users = dim_users_task.run()
    logger.info("DimUsers transformation complete.")

    dim_user_levels_task = DimUserLevelsTransformerTask(spark, user_levels)
    dim_user_levels = dim_user_levels_task.run()
    logger.info("DimUserLevels transformation complete.")

    dim_products_task = DimProductsTransformerTask(spark)
    dim_products = dim_products_task.run()
    logger.info("DimProducts transformation complete.")

    dim_jurisdictions_task = DimJurisdictionsTransformerTask(spark)
    dim_jurisdictions = dim_jurisdictions_task.run()
    logger.info("DimJurisdictions transformation complete.")

    # Step 3: Transform fact tables
    fact_deposits_task = FactDepositsTransformerTask(spark, deposits)
    fact_deposits = fact_deposits_task.run()
    logger.info("FactDeposits transformation complete.")

    fact_withdrawals_task = FactWithdrawalsTransformerTask(spark, withdrawals)
    fact_withdrawals = fact_withdrawals_task.run()
    logger.info("FactWithdrawals transformation complete.")

    fact_user_activity_task = FactUserActivityTransformerTask(spark, deposits, withdrawals, events)
    fact_user_activity = fact_user_activity_task.run()
    logger.info("FactUserActivity transformation complete.")

    # fact_staking_task = FactStakingTransformerTask(spark)
    # fact_staking = fact_staking_task.run()
    # logger.info("FactStaking transformation complete.")

    # Step 4: Save all tables
    save_dim_date_task = SaveTableTask(spark, dim_date, OUTPUT_PATH, "DimDate", format="csv")
    save_dim_date_task.run()
    logger.info("DimDate saved.")

    save_dim_users_task = SaveTableTask(spark, dim_users, OUTPUT_PATH, "DimUsers", format="csv")
    save_dim_users_task.run()
    logger.info("DimUsers saved.")

    save_dim_user_levels_task = SaveTableTask(spark, dim_user_levels, OUTPUT_PATH, "DimUserLevels", format="csv")
    save_dim_user_levels_task.run()
    logger.info("DimUserLevels saved.")

    save_dim_products_task = SaveTableTask(spark, dim_products, OUTPUT_PATH, "DimProducts", format="csv")
    save_dim_products_task.run()
    logger.info("DimProducts saved.")

    save_dim_jurisdictions_task = SaveTableTask(spark, dim_jurisdictions, OUTPUT_PATH, "DimJurisdictions", format="csv")
    save_dim_jurisdictions_task.run()
    logger.info("DimJurisdictions saved.")

    save_fact_deposits_task = SaveTableTask(spark, fact_deposits, OUTPUT_PATH, "FactDeposits", format="csv")
    save_fact_deposits_task.run()
    logger.info("FactDeposits saved.")

    save_fact_withdrawals_task = SaveTableTask(spark, fact_withdrawals, OUTPUT_PATH, "FactWithdrawals", format="csv")
    save_fact_withdrawals_task.run()
    logger.info("FactWithdrawals saved.")

    save_fact_user_activity_task = SaveTableTask(spark, fact_user_activity, OUTPUT_PATH, "FactUserActivity", format="csv")
    save_fact_user_activity_task.run()
    logger.info("FactUserActivity saved.")

    # save_fact_staking_task = SaveTableTask(spark, fact_staking, OUTPUT_PATH, "FactStaking", format="csv")
    # save_fact_staking_task.run()
    # logger.info("FactStaking saved.")

    logger.info("Pipeline completed successfully.")

if __name__ == "__main__":
    main()
