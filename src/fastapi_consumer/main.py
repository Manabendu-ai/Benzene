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

async def poll_consumer(consumer: KafkaConsumer):
    try:
        while not stop_polling_event.is_set():
            print("Trying to poll again!")
            records = consumer.poll(3000, 500)
            if records:
                for record in records.values():
                    for message in record:
                        msg = json.loads(message.value).get("message")
                        print(f"Recieved the message : {msg}\nFrom the Topic : {message.topic}")                    
            await asyncio.sleep(2)
    except Exception as e:
        print(f"Exception in Consuming {e}")
    finally:
        print(f"[INFO] closing consumer....")
        consumer.close()

task_list = []
@app.get("/benzene/consumer/trigger", tags=["Benzene Consumer"])
async def trigger_polling():
    if not task_list:
        stop_polling_event.clear()
        consumer = create_kafka_consumer()
        task = asyncio.create_task(poll_consumer(consumer=consumer))
        task_list.append(task)

        return {
            "status" : "Kafka Polling has Started!"
        }
    return {
        "status" : "Kafka Polling already triggered!"
    }

@app.get("/benzene/consumer/stop-trigger", tags=["Benzene Consumer"])
async def stop_trigger():
    stop_polling_event.set()
    if task_list:
        task_list.pop()

    return {
         "status" : "Kafka Polling stopped!"
    }