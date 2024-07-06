SET hive.metastore.warehouse.dir;
create database retail_DWH;

CREATE EXTERNAL TABLE IF NOT EXISTS retail_DWH.Customer_Dim (
  cust_surKey INT,
  customer_id INT,
  fname STRING,
  lname STRING,
  email String
)
STORED AS ORC;

CREATE EXTERNAL TABLE IF NOT EXISTS retail_DWH.Product_Dim (
  product_surKey INT,
  product_id INT,
  product_name STRING,
  category STRING
)
STORED AS ORC;

CREATE EXTERNAL TABLE IF NOT EXISTS retail_DWH.Branches_Dim (
  branch_surKey INT,
  branch_id INT,
  location STRING,
  class STRING,
  establish_date DATE
)
STORED AS ORC;

CREATE EXTERNAL TABLE IF NOT EXISTS retail_DWH.sales_agents_Dim (
  agent_surKey INT,
  agent_id INT,
  agent_name STRING,
  hire_date DATE
)
STORED AS ORC;

CREATE EXTERNAL TABLE IF NOT EXISTS retail_DWH.date_Dim (
  date_surKey BIGINT,
  datee DATE,
  year STRING,
  Month STRING,
  Day_of_week STRING,
  week_of_year STRING,
  week_day STRING,
  quarter STRING,
  day_name STRING,
  month_name STRING,
  is_weekend STRING
)
STORED AS ORC;

CREATE EXTERNAL TABLE IF NOT EXISTS retail_DWH.branches_TRX_fact (
  transcation_id STRING,
  branch_surKey INT,
  product_surKey INT,
  cust_surKey INT,
  agent_surKey INT,
  date_surKey BIGINT,
  units INT,
  discount_percentage string,
  total_price DECIMAL(15,5),
  payment_method STRING
)
STORED AS ORC;