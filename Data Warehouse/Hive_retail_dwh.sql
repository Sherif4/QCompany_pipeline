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
  discount_perc INT,
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


-------------------------------------------------------------------------------------------------------------------------------------------
SELECT Product_Dim.product_id AS productId,
           COUNT(*) AS count
    FROM retail_DWH.branches_TRX_fact
    JOIN retail_DWH.Product_Dim 
    ON branches_TRX_fact.product_sur_key = product_dim.product_sur_key
    join retail_DWH.online_TRX_fact
    on online_TRX_fact.product_sur_key=product_dim.product_sur_key
    GROUP BY product_dim.product_id
    order by count desc
    limit 5;

---------------------------------------------------------------------------------------------------------------------------------------------
#most redeemed offers from customer
SELECT Customer_Dim.customer_id,
           COUNT(*) AS count,
           branches_TRX_fact.discount_perc,
           online_TRX_fact.discount_perc
    FROM branches_TRX_fact
    JOIN Customer_Dim 
    ON branches_TRX_fact.customer_sur_key = customer_dim.customer_sur_key
    join online_TRX_fact
    on online_TRX_fact.customer_sur_key=customer_dim.customer_sur_key
    where branches_TRX_fact.discount_perc != 0 and online_TRX_fact.discount_perc != 0
    GROUP BY branches_TRX_fact.discount_perc,online_TRX_fact.discount_perc,Customer_Dim.customer_id
    order by count desc
    limit 5;
--------------------------------------------------------------------------------------------------------------------------------------------
#most redeemed offers per product
SELECT product_dim.product_id,
           COUNT(*) AS count,
           branches_TRX_fact.discount_perc,
           online_TRX_fact.discount_perc
    FROM branches_TRX_fact
    JOIN product_dim 
    ON branches_TRX_fact.product_sur_key = product_dim.product_sur_key
    join online_TRX_fact
    on online_TRX_fact.product_sur_key=product_dim.product_sur_key
    where branches_TRX_fact.discount_perc != 0 and online_TRX_fact.discount_perc != 0
    GROUP BY branches_TRX_fact.discount_perc,online_TRX_fact.discount_perc,product_dim.product_id
    order by count desc
    limit 5;
---------------------------------------------------------------------------------------------------------------------------------------------
#lowest cities 
select city,
    count(transaction_id) as CountCity
    from online_TRX_fact
    group by city
    order by CountCity asc;