from utils.postgres_helper import AsyncPostgreSQLHelper
from utils.kafka_producer import AsyncKafkaProducer
from utils.sql.base_sql import SQL
from functools import wraps
from config import Config



def async_provide_kafka_connection():
    def wrapper(func):

        @wraps(func)
        async def wrapped(*args, **kwargs):
            kafka_transaction_session = None
            async_kafka_producer = None

            try:
                async_kafka_producer = AsyncKafkaProducer(
                        host=Config.get('KAFKA_HOST'),
                        port=Config.get('KAFKA_PORT')
                    )
                
                kafka_transaction_session = await async_kafka_producer.start()
                
                await func(kafka_session = async_kafka_producer , *args, **kwargs)
            
            except Exception as e:
                print("Failed to Connect Apache Kafka")
                raise e
            
            finally:
                if kafka_transaction_session and async_kafka_producer:
                    await async_kafka_producer.stop()
            
                return 
            
        return wrapped
    return wrapper



def async_provide_postgres_connection():
    def wrapper(func):

        @wraps(func)
        async def wrapped(*args, **kwargs):
            session = None
            async_postgres_helper = None 

            try:
                async_postgres_helper = AsyncPostgreSQLHelper(
                        host=Config.get('POSTGRES_HOST'),
                        port=Config.get('POSTGRES_PORT'),
                        dbname=Config.get('POSTGRES_DATABASE'),
                        user=Config.get('POSTGRES_USER'),
                        password=Config.get('POSTGRES_PASSWORD')
                    )
                
                session = await async_postgres_helper.connect()
                await func(postgres_session = async_postgres_helper , *args, **kwargs)
            except Exception as e:

                print(f'Error occured while connecting to databaase: {e} ')


            finally:
                if session and async_postgres_helper:
                    await async_postgres_helper.close_connection()

            return 
        
            
        return wrapped
    return wrapper


 
def log_to_postgres():
    def wrapper(func):

        @wraps(func)
        @async_provide_postgres_connection()
        async def wrapped(*args, **kwargs):

            session = kwargs.get('postgres_session')

            if not session:
                raise Exception('Failed to connect')
            
            try:
                job_record = await session.fetch_one(SQL.read_sql("""insert into "Reddit"."SubredditJob"("JobStatus")  values('started') returning "SubredditJobId"  """))

                job_id = job_record.get('SubredditJobId')

                await func(subreddit_job_id = job_id,*args, **kwargs)

            except Exception as e:
                await session.execute_query(SQL.read_sql("""update "Reddit"."SubredditJob" set 
                                        "JobEndDate" = now(),
                                        "JobStatus" = 'fail' 
                                        WHERE "SubredditJobId" = {job_id} """,  job_id = job_id))
                
            else:

                await session.execute_query(SQL.read_sql("""update "Reddit"."SubredditJob" set 
                                        "JobEndDate" = now(),
                                        "JobStatus" = 'success' 
                                        WHERE "SubredditJobId" = {job_id} """,  job_id = job_id))

                

            return True

        return wrapped
    return wrapper



