set hive.metastore.warehouse.dir;
CREATE DATABASE IF NOT EXISTS retail_DWH;

-- Customer Dimension Staging Table
CREATE EXTERNAL TABLE IF NOT EXISTS retail_DWH.Customer_Dim_staging (
  customer_sur_key INT,
  customer_id BIGINT,
  customer_fname STRING,
  customer_lname STRING,
  customer_email STRING
)
STORED AS PARQUET
LOCATION 'hdfs:///data/retail_gold/customer_dim/';

-- Product Dimension Staging Table
CREATE EXTERNAL TABLE IF NOT EXISTS retail_DWH.Product_Dim_staging (
  product_sur_key INT,
  product_id BIGINT,
  product_name STRING,
  product_category STRING
)
STORED AS PARQUET
LOCATION 'hdfs:///data/retail_gold/product_dim/';

-- Branches Dimension Staging Table
CREATE EXTERNAL TABLE IF NOT EXISTS retail_DWH.Branches_Dim_staging (
  branch_sur_key INT,
  branch_id BIGINT,
  location STRING,
  establish_date DATE,
  class STRING
)
STORED AS PARQUET
LOCATION 'hdfs:///data/retail_gold/branches_dim/';

-- Sales Agents Dimension Staging Table
CREATE EXTERNAL TABLE IF NOT EXISTS retail_DWH.sales_agents_Dim_staging (
  sales_agent_sur_key INT,
  sales_person_id BIGINT,
  name STRING,
  hire_date DATE
)
STORED AS PARQUET
LOCATION 'hdfs:///data/retail_gold/sales_agent_dim/';

-- Date Dimension Staging Table
CREATE EXTERNAL TABLE IF NOT EXISTS retail_DWH.date_Dim_staging (
  datee DATE,
  year INT,
  month INT,
  day INT,
  week INT,
  weekday INT,
  quarter INT,
  day_name STRING,
  month_name STRING,
  is_weekend INT,
  date_sur_key BIGINT
)
STORED AS PARQUET
LOCATION 'hdfs:///data/retail_gold/date_dim/';

-- Branches Transaction Fact Table
CREATE EXTERNAL TABLE IF NOT EXISTS retail_DWH.branches_TRX_fact_staging (
  transaction_id STRING,
  branch_sur_key INT,
  product_sur_key INT,
  customer_sur_key INT,
  sales_agent_sur_key INT,
  date_sur_key BIGINT,
  units BIGINT,
  unit_price double,
  discount_perc INT,
  total_price double,
  payment_method STRING
)
STORED AS PARQUET
LOCATION 'hdfs:///data/retail_gold/offline_fact/';

-- Online Transaction Fact Table
CREATE EXTERNAL TABLE IF NOT EXISTS retail_DWH.online_TRX_fact_staging (
  transaction_id STRING,
  units BIGINT,
  unit_price double,
  payment_method STRING,
  discount_perc INT,
  total_price Double,
  customer_sur_key INT,
  product_sur_key INT,
  date_sur_key BIGINT,
  street STRING,
  city STRING,
  state STRING,
  postal_code STRING
)
STORED AS PARQUET
LOCATION 'hdfs:///data/retail_gold/online_fact/';
----------------------------------------------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS retail_DWH.Customer_Dim (
  customer_sur_key INT,
  customer_id BIGINT,
  customer_fname STRING,
  customer_lname STRING,
  customer_email String
)
STORED AS ORC;

CREATE EXTERNAL TABLE IF NOT EXISTS retail_DWH.Product_Dim (
  product_sur_key INT,
  product_id BIGINT,
  product_name STRING,
  product_category STRING
)
STORED AS ORC;

CREATE EXTERNAL TABLE IF NOT EXISTS retail_DWH.Branches_Dim (
  branch_sur_key INT,
  branch_id BIGINT,
  location STRING,
  establish_date DATE,
  class STRING
)
STORED AS ORC;

CREATE EXTERNAL TABLE IF NOT EXISTS retail_DWH.sales_agents_Dim (
  sales_agent_sur_key INT,
  sales_person_id BIGINT,
  name STRING,
  hire_date DATE
)
STORED AS ORC;

CREATE EXTERNAL TABLE IF NOT EXISTS retail_DWH.date_Dim (
  datee DATE,
  year INT,
  Month INT,
  Day_of_week INT,
  week_of_year INT,
  week_day INT,
  quarter BIGINT,
  day_name STRING,
  month_name STRING,
  is_weekend INT,
  date_sur_key BIGINT
)
PARTITIONED BY (year STRING)
STORED AS ORC;

CREATE EXTERNAL TABLE IF NOT EXISTS retail_DWH.branches_TRX_fact (
  transcation_id STRING,
  branch_sur_key INT,
  product_sur_key INT,
  customer_sur_key INT,
  sales_agent_sur_key INT,
  date_sur_key BIGINT,
  units BIGINT,
  unit_price DECIMAL(15,5),
  discount_percentage INT,
  total_price DECIMAL(15,5),
  insertion_date BIGINT
)
PARTITIONED BY (payment_method STRING)
STORED AS ORC;

CREATE EXTERNAL TABLE IF NOT EXISTS retail_DWH.online_TRX_fact (
  transaction_id STRING,
  units BIGINT,
  unit_price DECIMAL(15,5),
  discount_perc INT,
  total_price DECIMAL(15,5),
  customer_sur_key INT,
  product_sur_key INT,
  date_sur_key BIGINT,
  street STRING,
  city STRING,
  state STRING,
  postal_code STRING,
  insertion_date BIGINT
)
PARTITIONED BY (payment_method STRING)
STORED AS ORC;