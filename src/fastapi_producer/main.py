from fastapi import FastAPI, BackgroundTasks
from kafka.admin import KafkaAdminClient, NewTopic
from kafka_producer import produce_kafka_message
from contextlib import asynccontextmanager
from producer_schema import ProducerMessage


KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "benzene-topic"
KAFKA_ADMIN_CLIENT = "benzene-admin-client"