## Kafka Producer for Customer Events

### Dependencies

* `confluent-kafka`: Python library for interacting with Apache 
* `json`: Python library for working with JSON data
* `time`: Python library for working with time-related functionalities
* `random`: Python library for generating random numbers 

### Configuration

The script utilizes a dictionary named `conf` to define producer configuration details:

```python
conf = {
    'bootstrap.servers': '****',
    'security.protocol': '****',
    'sasl.mechanisms': '****',
    'sasl.username': '****',
    'sasl.password': '****',
}
```

* `bootstrap.servers`: The address of the Confluent Cloud Kafka cluster.
* `security.protocol`: Security protocol used for communication (SASL_SSL in this case).
* `sasl.mechanisms`: Authentication mechanism (PLAIN here).
* `sasl.username`: Username for Confluent Cloud access.
* `sasl.password`: Password for Confluent Cloud access 

**Note:** Replace the placeholder values for `sasl.username` and `sasl.password` with your actual Confluent Cloud credentials.

### Topic Name

The script defines a variable `topic` to specify the Kafka topic name where events will be produced.

```python
# add topic_name here 
topic = '' # add topic name
```

### Event Generation (`generate_event` function)

The `generate_event` function takes an `event_type` string as input and returns a dictionary representing a customer event. 

* The event dictionary includes common fields like `eventType`, `customerId`, `productId`, `timestamp`, and `metadata`.
* The `metadata` field can contain additional details specific to the event type.
* The function populates additional fields based on the provided `event_type`:
    * `productView`: Adds `category` and `source` to `metadata`.
    * `addToCart`: Includes `quantity`.
    * `purchase`: Adds `quantity`, `totalAmount`, and `paymentMethod`.
    * `recommendationClick`: Includes `recommendedProductId` and `algorithm`.

### Event Producer (`Producer` object)

The script creates a `Producer` object from the `confluent_kafka.Producer` class using the configuration dictionary `conf`. This object is responsible for sending events to the Kafka topic.

### Sending Events (`send_event` function)

The `send_event` function takes an event dictionary as input and attempts to send it to the Kafka topic:

* It converts the event dictionary to a JSON string using `json.dumps`.
* The function uses the producer object's `produce` method to send the JSON data to the specified topic.
* Upon successful production, the function prints a confirmation message with the event details.
* Any exceptions encountered during production are caught and printed as errors.

### Main Loop (`main` function)

The `main` function runs continuously:

* It randomly selects an `event_type` from a predefined list.
* It calls `generate_event` to create a corresponding event object.
* The event details are printed for reference.
* The `send_event` function is called to send the event to Kafka.
* The script introduces a random sleep between events using `time.sleep` to simulate real-world event generation patterns.

### Running the Script

1. Ensure you have the required libraries installed (`confluent-kafka`, `json`, `time`, `random`).
2. Replace the placeholder values in `conf` with your Confluent Cloud credentials.
3. Set the desired topic name in the `topic` variable.
4. Run the script using Python (`python producer.py`).
