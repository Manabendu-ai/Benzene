from kafka import KafkaProducer
from fastapi import HTTPException
from producer_schema import ProducerMessage
import json

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "benzene-topic"
PRODUCER_CLIENT_ID = "benzene_producer"
