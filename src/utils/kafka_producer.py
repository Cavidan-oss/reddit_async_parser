import json
from aiokafka import AIOKafkaProducer
import asyncio

class AsyncKafkaProducer:
    def __init__(self,  port, host ='localhost', loop=None):
        self.host = host
        self.port = port

        self.bootstrap_servers = f"{self.host}:{self.port}"
        self.loop = loop or asyncio.get_event_loop()

        self.producer = AIOKafkaProducer(
            loop=self.loop,
            bootstrap_servers=self.bootstrap_servers,
        )

    async def start(self):
        
        await self.producer.start()
        return True

    async def stop(self):
        await self.producer.stop()
        print("Kafka Connection closed")

    async def push_to_kafka(self,topic, message):
        try:
            # Produce message to Kafka topic
            await self.producer.send(topic, json.dumps(message).encode('utf-8', errors='ignore'))
            # print(f"Message sent to {topic}: {message}")
        except Exception as e:
            print(f"Error while pushing message to Kafka: {e}")