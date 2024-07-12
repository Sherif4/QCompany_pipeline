CREATE EXTERNAL TABLE streaming_data (
    eventType STRING,
    customerId STRING,
    productId STRING,
    `timestamp` TIMESTAMP,
    metadata STRUCT<category: STRING, source: STRING>,
    quantity INT,
    totalAmount FLOAT,
    paymentMethod STRING,
    recommendedProductId STRING,
    algorithm STRING
)
STORED AS PARQUET
LOCATION 'hdfs://your-hdfs-path';