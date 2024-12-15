
## **Overview**
This README provides an in-depth explanation of the data model, ETL scripts, and techniques used to solve Bitso Challenge 2 problem. It includes the rationale behind the modeling decisions, detailed descriptions of the tables, the data pipeline for generating them, and a proposal for ingesting the data into a Data Lake using PySpark.

---

## **Data Model**
The data model is designed to address Bitso’s evolving requirements, including:
- **Geographic expansion** (e.g., new jurisdictions and regulations).
- **Non-peer-to-peer transactions** (e.g., internal activities like earning rewards).

### **Modeling Techniques**
We employed a **hybrid schema** that combines **star schema** principles with elements of **snowflake schema** and bridge tables for flexibility and scalability.

#### **Why This Approach?**
1. **Star Schema Elements**:
   - Simplifies analytical queries with denormalized fact tables linked to descriptive dimension tables.
   - Fact tables (e.g., deposits, withdrawals, user activities) are optimized for performance.
2. **Snowflake Schema Elements**:
   - Normalized dimensions (e.g., `DimProducts`, `DimJurisdictions`, `DimUserLevels`) reduce redundancy.
   - Historical tracking for changing attributes like user levels is easier with normalized tables.
3. **Bridge Tables**:
   - The `ProductJurisdiction` table resolves the many-to-many relationship between products and jurisdictions.
4. **Extensibility**:
   - Modular design allows easy addition of new products, activities, or regions.

---

### **Data Model Overview**
The model comprises both **fact tables** and **dimension tables**:

#### **Fact Tables**
1. **FactDeposits**:
   - Stores completed deposit transactions.
   - **Columns**:
     - `user_id` (FK to `DimUsers`)
     - `deposit_date` (FK to `DimDate`)
     - `amount`
     - `currency_id` (FK to `DimCurrencies`)
   - **Purpose**: Optimized for queries like total deposits, average deposits, and currency breakdowns.

2. **FactWithdrawals**:
   - Tracks completed withdrawal transactions.
   - **Columns**:
     - `user_id` (FK to `DimUsers`)
     - `withdrawal_date` (FK to `DimDate`)
     - `amount`
     - `currency_id` (FK to `DimCurrencies`)
   - **Purpose**: Simplifies withdrawal analytics.

3. **FactUserActivity**:
   - Logs user activities (deposits, withdrawals, logins).
   - **Columns**:
     - `activity_id`
     - `user_id` (FK to `DimUsers`)
     - `activity_date` (FK to `DimDate`)
     - `activity_type` (e.g., `deposit`, `login`)
     - `amount` (nullable)
     - `currency_id` (nullable FK to `DimCurrencies`)
   - **Purpose**: Supports queries about active users and cross-event analysis.

#### **Dimension Tables**
1. **DimUsers**:
   - Describes users.
   - **Columns**:
     - `user_id`
     - `registration_date` (FK to `DimDate`)
     - `jurisdiction`
   - **Purpose**: Supports user segmentation and jurisdiction analysis.

2. **DimUserLevels**:
   - Tracks changes in user levels over time.
   - **Columns**:
     - `user_id` (FK to `DimUsers`)
     - `jurisdiction`
     - `level`
     - `effective_date` (FK to `DimDate`)
   - **Purpose**: Tracks historical level changes.

3. **DimDate**:
   - Provides temporal attributes for date-based analysis.
   - **Columns**:
     - `date_id`
     - `calendar_date`, `year`, `month`, `day`, `weekday`
   - **Purpose**: Simplifies time-based queries.

4. **DimCurrencies**:
   - Stores currency details.
   - **Columns**:
     - `currency_id`
     - `currency_code`
   - **Purpose**: Enables currency-specific filtering and aggregation.

5. **DimProducts**:
   - Catalogs products
   - **Columns**:
     - `product_id`
     - `product_name`
     - `product_type`
     - `availability`
     - `launch_date`
   - **Purpose**: Supports product-based analytics.

6. **DimJurisdictions**:
   - Catalogs regional details.
   - **Columns**:
     - `jurisdiction_id`
     - `country_code`
     - `jurisdiction_name`
     - `currency`
     - `regulatory_info`
   - **Purpose**: Supports jurisdiction-specific queries.

7. **ProductJurisdiction (Bridge Table)**:
   - Links products to regions.
   - **Columns**:
     - `product_id` (FK to `DimProducts`)
     - `jurisdiction_id` (FK to `DimJurisdictions`)
     - `availability_date`
   - **Purpose**: Tracks product availability across regions.

---

## **ETL Process**
The ETL pipeline is implemented using **PySpark**, leveraging distributed processing for scalability.

### **ETL Steps**:
1. **Extract**:
   - Load raw data from CSV files using `spark.read.csv`.
   - Ensure schema inference and parsing of timestamps.

2. **Transform**:
   - **Fact Tables**:
     - Filter for valid transactions (e.g., `tx_status = 'complete'`).
     - Enrich with foreign keys (e.g., map currencies to `DimCurrencies`).
   - **Dimension Tables**:
     - Deduplicate records (e.g., `DimUsers`).
     - Generate synthetic keys (e.g., `date_id` in `DimDate`).
   - **Bridge Table**:
     - Manually define product-region relationships.

3. **Load**:
   - Save tables to the specified output directory in Parquet or CSV format for ingestion into the DWH/Data Lake.

---

## **Technologies for Data Ingestion**
### **Data Lake Approach**:
1. **Storage**:
   - Use AWS S3, Azure Data Lake, or Google Cloud Storage for raw, processed, and enriched data.
2. **Format**:
   - Store raw data in CSV/JSON and processed data in Parquet/ORC for optimized query performance.
3. **Query Engine**:
   - Use Presto or AWS Athena for interactive SQL queries over the lake.
4. **ETL Orchestration**:
   - Use Apache Airflow or Spark Structured Streaming for automated workflows.

### **Data Warehouse Approach**:
1. **Database**:
   - Use Snowflake, BigQuery, or Amazon Redshift for analytical workloads.
2. **Schema Design**:
   - Partition fact tables by date or jurisdiction for performance.
   - Index high-query columns (e.g., `user_id`, `date_id`).
3. **Pipeline**:
   - Write PySpark output directly to the DWH using JDBC connectors.
   - Schedule periodic updates using Apache Airflow.

---

## **Pros and Cons of the Approach**
### **Pros**:
- **Scalability**: Supports new products, regions, and transaction types.
- **Flexibility**: Modular design allows incremental additions without disrupting existing structures.
- **Performance**: Optimized for analytical queries with pre-aggregated fact tables.
- **Historical Tracking**: Supports tracking temporal changes (e.g., user levels).

### **Cons**:
- **Complexity**: Requires careful maintenance of bridge tables and foreign key mappings.
- **ETL Overhead**: Distributed pipelines add computational and operational costs.
- **Storage Requirements**: Denormalized fact tables increase storage usage.

---
## **Running the script**
### **Pros**:
- **Scalability**: Supports new products, regions, and transaction types.
- **Flexibility**: Modular design allows incremental additions without disrupting existing structures.
- **Performance**: Optimized for analytical queries with pre-aggregated fact tables.
- **Historical Tracking**: Supports tracking temporal changes (e.g., user levels).

### **Cons**:
- **Complexity**: Requires careful maintenance of bridge tables and foreign key mappings.
- **ETL Overhead**: Distributed pipelines add computational and operational costs.
- **Storage Requirements**: Denormalized fact tables increase storage usage.

---

Here’s a small section you can add to your README:

---

### **How to Run the Script**

1. **Create and Activate a Virtual Environment**  
   Create a virtual environment and activate it:

   ```bash
   python3 -m venv etl_env
   source etl_env/bin/activate
   ```

2. **Install Dependencies**  
   Install the required Python packages listed in `requirements.txt`:

   ```bash
   pip install -r requirements.txt
   ```

3. **Run the Script**  
   Set the `PYTHONPATH` to the project root and run the script:

   ```bash
   export PYTHONPATH=.
   python3 -m core.main
   ```

   This will execute the main script located in the `core` module.

4. **Deactivate the Virtual Environment**  
   When you're done, deactivate the virtual environment:

   ```bash
   deactivate
   ```

---


### **Project structure**

1. **/core**  
   - main.py: Orchestrates the pipeline
   - /base: Base Spark task holding basic logic
   - /read: Read-related tasks
   - /transform: Transformation-related tasks for generating the dim/fact tables
   - /write: Write-related tasks
   - /output: Folder containing the output tables
   - /queries: SQL queries for answering Bitso asks
   - /resources: Input data (Sample -> 1% of data, Full: 100% of data)
   *ps: The output was too big so a link with the data will be sent

2. **Install Dependencies**  
   Install the required Python packages listed in `requirements.txt`:

   ```bash
   pip install -r requirements.txt
   ```

3. **Run the Script**  
   Set the `PYTHONPATH` to the project root and run the script:

   ```bash
   export PYTHONPATH=.
   python3 -m core.main
   ```

   This will execute the main script located in the `core` module.

4. **Deactivate the Virtual Environment**  
   When you're done, deactivate the virtual environment:

   ```bash
   deactivate
   ```

---
