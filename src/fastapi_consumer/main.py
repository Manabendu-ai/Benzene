from fastapi import FastAPI
import asyncio
from kafka import KafkaConsumer
import json

KAFKA_BROKER="localhost:9092"
KAFKA_topic="benzene-topic"
KAFKA_CONSUMER_ID="benzene_consumer"