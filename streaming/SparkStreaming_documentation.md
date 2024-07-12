## Spark Streaming with Kafka

**Kafka Connection Details:**

```python
bootstrap_servers = "****"
kafka_topic = "***" # add topic name
kafka_username = "****"
kafka_password = "****"
```

This section defines essential details for connecting to the Kafka cluster:

* **`bootstrap_servers`**: This variable stores the address of the Kafka bootstrap server, which acts as the entry point for connecting to the Kafka cluster.
* **`kafka_topic`**: This specifies the name of the Kafka topic from which the streaming data will be consumed.
* **`kafka_username` and `kafka_password`**: These variables hold the credentials for secure access to the Kafka topic. 
  * **Note:** Avoid storing sensitive information like passwords directly in code. Consider environment variables.

**Define JSON Schema:**

```python
schema = StructType() \
  .add("eventType", StringType()) \
  .add("customerId", StringType()) \
  .add("productId", StringType()) \
  .add("timestamp", TimestampType()) \
  .add("metadata", StructType()
      .add("category", StringType())
      .add("source", StringType())
  ) \
  .add("quantity", IntegerType()) \
  .add("totalAmount", FloatType()) \
  .add("paymentMethod", StringType()) \
  .add("recommendedProductId", StringType()) \
  .add("algorithm", StringType())
```

This code snippet defines the schema for the incoming JSON data. A schema acts as a blueprint, specifying the expected structure and data types of each field within the JSON objects received from the Kafka topic.

**Read Stream from Kafka:**

```python
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", bootstrap_servers) \
  .option("subscribe", kafka_topic) \
  .option("startingOffsets", "earliest") \
  .option("kafka.security.protocol", "SASL_SSL") \
  .option("kafka.sasl.mechanism", "PLAIN") \
  .option("kafka.sasl.jaas.config",
      f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_username}" password="{kafka_password}";') \
  .load()
```

This section configures a Spark Streaming DataFrame to continuously read data from the specified Kafka topic.

- **format("kafka")**: Specify Kafka as the data source.
- **option("kafka.bootstrap.servers", bootstrap_servers)**: Set the Kafka broker address.
- **option("subscribe", kafka_topic)**: Specify the topic to subscribe to.
- **option("startingOffsets", "earliest")**: Read data from the beginning of the topic.
- **option("kafka.security.protocol", "SASL_SSL")**: Use SASL_SSL for security.
- **option("kafka.sasl.mechanism", "PLAIN")**: Use the PLAIN mechanism for SASL.
- **option("kafka.sasl.jaas.config", ...)**: Provide the Kafka credentials.

### Process JSON Data

```python
json_df = df.selectExpr("CAST(value AS STRING)").select(from_json("value", schema).alias("data")).select("data.*")
```
Convert the Kafka value column from binary to string, parse it as JSON, and select the individual fields defined in the schema.

### Write Data to HDFS

```python
query = json_df \
  .writeStream \
  .outputMode("append") \
  .format("parquet") \
  .option("path", "hdfs://your-hdfs-path/streaming-output") \
  .option("checkpointLocation", "hdfs://your-hdfs-path/checkpoint") \
  .start()
```

- **outputMode("append")**: Append the new data to the existing dataset.
- **format("parquet")**: Write the data in Parquet format.
- **option("path", "hdfs://your-hdfs-path/streaming-output")**: Specify the output path in HDFS.
- **option("checkpointLocation", "hdfs://your-hdfs-path/checkpoint")**: Specify the checkpoint location in HDFS.
- **start()**: Start the streaming query.

### Monitoring and Stopping the Query

The streaming query will run continuously, processing new data as it arrives. To stop the query, you can use:

```python
query.awaitTermination()
```

or stop it manually with:

```python
query.stop()
```

