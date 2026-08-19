from kafka import KafkaProducer
from fastapi import HTTPException
from .producer_schema import ProducerMessage
import json

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "benzene-topic"
PRODUCER_CLIENT_ID = "benzene_producer"

def serializer(message):
    return json.dumps(message).encode()

producer = KafkaProducer(
    api_version=(7,4,10),
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=serializer,
    client_id=PRODUCER_CLIENT_ID
)

def produce_kafka_message(messsage: ProducerMessage):
    try:
        producer.send(KAFKA_TOPIC, json.dumps({'message':messsage.message}))
        producer.flush()
    except Exception as err:
        print(err)
        raise HTTPException(status_code=500, detail="Failed to send the message")