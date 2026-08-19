from fastapi import FastAPI
import asyncio
from kafka import KafkaConsumer
import json

KAFKA_BROKER="localhost:9092"
KAFKA_TOPIC="benzene-topic"
KAFKA_CONSUMER_ID="benzene_consumer"

stop_polling_event = asyncio.Event()
app = FastAPI(
    title="Benzene-Consumer",
    description="An Event Driven Architechture",
    version="1.0.0",
)

def json_deserializer(value):
    if value is None:
        return None
    try:
        return json.loads(value.decode('utf-8'))
    except Exception as e:
        print("Unable to decode!")
        return None

def create_kafka_consumer():

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=KAFKA_CONSUMER_ID,
        value_deserializer=json_deserializer
    )

    return consumer