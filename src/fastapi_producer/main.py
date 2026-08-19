from fastapi import FastAPI, BackgroundTasks
from kafka.admin import KafkaAdminClient, NewTopic
from .kafka_producer import produce_kafka_message
from contextlib import asynccontextmanager
from .producer_schema import ProducerMessage


KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "benzene-topic"
KAFKA_ADMIN_CLIENT = "benzene-admin-client"

@asynccontextmanager
async def lifespan(app: FastAPI):

    admin_client = KafkaAdminClient(
        bootstrap_servers=KAFKA_BROKER,
        client_id=KAFKA_ADMIN_CLIENT
    )

    if not KAFKA_TOPIC in admin_client.list_topics():
        admin_client.create_topics(
            new_topics=[
                NewTopic(
                    name=KAFKA_TOPIC,
                    num_partitions=1,
                    replication_factor=1
                )
            ],
            validate_only=False
        )

    yield

app = FastAPI(lifespan=lifespan)

@app.post("/benzene/produce/message", tags=['Benzene Producer'])
async def produce_message(message : ProducerMessage, background_task : BackgroundTasks):
    background_task.add_task(produce_kafka_message, message)
    return {
        "app": {
            "name" : "benzene",
            "message" : message,
            "response" : "Message Recieved, ThankYou for sending Message through Benzene!"
        }
    }
