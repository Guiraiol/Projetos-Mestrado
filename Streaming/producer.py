
import pandas as pd
import json
from kafka import KafkaProducer

KAFKA_INPUT = "input_topic"
KAFKA_OUTPUT = "output_topic"
KAFKA_SERVER = "127.0.0.1:9092"

kafka_producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER)
data_producer = pd.read_csv("iris_test.csv").to_dict(orient = "records")
print(len(data_producer))
for message in data_producer:
    print(message)
    kafka_producer.send(KAFKA_INPUT,  bytes(json.dumps(message, default=str).encode('utf-8')))
    input()