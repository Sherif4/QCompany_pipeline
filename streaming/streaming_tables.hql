set hive.metastore.warehouse.dir;
CREATE DATABASE IF NOT EXISTS streaming_dwh;

CREATE EXTERNAL TABLE streaming_dwh.recommendation_click (
    customerId STRING,
    productId STRING,
    `timestamp` TIMESTAMP,
    recommendedProductId STRING,
    algorithm STRING
)
STORED AS PARQUET
LOCATION 'hdfs:///data/golden_layer/recommendation_click';

CREATE EXTERNAL TABLE streaming_dwh.purchase (
    customerId STRING,
    productId STRING,
    `timestamp` TIMESTAMP,
    quantity INT,
    totalAmount DOUBLE,
    paymentMethod STRING
)
STORED AS PARQUET
LOCATION 'hdfs:///data/golden_layer/purchase';

CREATE EXTERNAL TABLE streaming_dwh.add_to_cart (
    customerId STRING,
    productId STRING,
    `timestamp` TIMESTAMP,
    quantity INT
)
STORED AS PARQUET
LOCATION 'hdfs:///data/golden_layer/add_to_cart';


CREATE EXTERNAL TABLE streaming_dwh.product_view (
    customerId STRING,
    productId STRING,
    `timestamp` TIMESTAMP,
    metadata STRUCT<category: STRING, source: STRING>
)
STORED AS PARQUET
LOCATION 'hdfs:///data/golden_layer/product_view';
