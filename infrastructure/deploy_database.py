import psycopg2 
import sys
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from src.config import Config
from aiokafka import AIOKafkaProducer
from aiokafka.errors import TopicAlreadyExistsError
import json
import asyncio


deployment_sql = """


CREATE SCHEMA IF NOT EXISTS "Reddit";


CREATE TABLE IF NOT EXISTS "Reddit"."Subreddit"
(
"SubredditId" serial,
"SubredditName" varchar(250),
"LastModifiedDate" timestamp WITH time ZONE DEFAULT NULL,
"IsActive" boolean DEFAULT TRUE,
"AddedDate" timestamp DEFAULT now(),
"DeletedDate" timestamp DEFAULT NULL
);


create table if not exists "Reddit"."SubredditSchedule"
(
 "SubredditScheduleId" serial,
 "SubredditId" integer,
 "LastParsedDate" timestamp with time ZONE ,
 "LastCheckStatus" varchar(30) default Null,
 "LastModifiedDate" timestamp with time ZONE DEFAULT NULL,
 "IsActive" boolean DEFAULT TRUE,
 "AddedDate" timestamp DEFAULT now(),
 "DeletedDate" timestamp DEFAULT NULL
);


create table if not exists "Reddit"."SubredditJob"
(
 "SubredditJobId" serial,
 "SubredditId" int default NULL,
 "JobTypeId" int default 1,
 "JobStartDate" timestamp default now() ,
 "JobEndDate" timestamp default null,
 "JobStatus" varchar(30) default 'started',
 "TopicId" int default NULL 
);


create table if not exists "Reddit"."KafkaTopic"
(
"TopicId" serial,
"TopicName" varchar(200),
 "LastModifiedDate" timestamp WITH time ZONE DEFAULT NULL,
 "IsActive" boolean DEFAULT TRUE,
 "AddedDate" timestamp DEFAULT now(),
 "DeletedDate" timestamp DEFAULT NULL
);


DROP TABLE IF EXISTS "Reddit"."JobType" ;

create table if not exists "Reddit"."JobType"
(
"JobTypeId" serial,
"JobTypeName" varchar(200),
 "LastModifiedDate" timestamp WITH time ZONE DEFAULT NULL,
 "IsActive" boolean DEFAULT TRUE,
 "AddedDate" timestamp DEFAULT now(),
 "DeletedDate" timestamp DEFAULT NULL
);




insert into "Reddit"."JobType"("JobTypeName") VALUES('scheduled');
insert into "Reddit"."JobType"("JobTypeName") VALUES('manual');



"""

def deploy_database():

    with psycopg2.connect(
            host=Config.get('POSTGRES_HOST'),
            database=Config.get('POSTGRES_DATABASE'),
            user=Config.get('POSTGRES_USER'),
            password=Config.get('POSTGRES_PASSWORD'),
            port = Config.get('POSTGRES_PORT')
        ) as connection:

        cursor = connection.cursor()

        cursor.execute(deployment_sql)


async def create_topic():

    try:
        producer = AIOKafkaProducer(
            bootstrap_servers=f"{Config.get('KAFKA_HOST')}:{Config.get('KAFKA_PORT')}"
        )

        # Start the producer
        await producer.start()
        # Create a topic
        topic = Config.get('DEFAULT_KAFKA_TOPIC')

        # Create the topic
        await producer.send(topic, json.dumps({}).encode('utf-8', errors='ignore'))
        print(f"Topic '{topic}' created successfully.")

    except TopicAlreadyExistsError:
        print(f"Topic '{topic}' already exists.")
    finally:
        # Stop the producer
        await producer.stop()


# Run the create_topic coroutine











if __name__ == '__main__':   
    deploy_database()

    asyncio.run(create_topic())