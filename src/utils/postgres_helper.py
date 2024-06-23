import asyncpg
from .sql.query import QueryStorage
from .sql.base_sql import SQL



class AsyncPostgreSQLHelper:
    
    def __init__(self, dbname, user, password, host="localhost", port=5432):
        self.dbname = dbname if dbname else ''
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.connection = None


    async def connect(self):

        conn_string = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"

        self.connection = await asyncpg.connect(
            conn_string
        )

        return self.connection

    async def process_subreddit_conf(self,subreddit_path):

        query = SQL.read_sql(QueryStorage.check_for_schedule_existance, subreddit_path = subreddit_path)

        result = await self.fetch_one(query)
        result = result if result else {}

        sub_id, subreddit_schedule_id, sub_parsed_date = result.get('SubredditId') ,  result.get('SubredditScheduleId') , result.get('LastParsedDate')   
        
        schedule_status = await self.check_for_schedule(sub_id, subreddit_schedule_id, sub_parsed_date)

        if schedule_status:
            return (sub_id, subreddit_schedule_id, sub_parsed_date)

        if not sub_id:
            sub_id = await self.insert_subreddit(subreddit_path)

        
        if not subreddit_schedule_id:
            subreddit_schedule_id = await self.insert_schedule(sub_id)


        return (sub_id, subreddit_schedule_id, None)

    async def process_kafka_topic(self, topic_name):
        topic_id = None 

        try:

            kafka_topic_result = await self.fetch_one(SQL.read_sql(QueryStorage.kafka_topic_select_query, topic_name = topic_name))
            kafka_topic_result = kafka_topic_result if kafka_topic_result else {}

            topic_id = kafka_topic_result.get("TopicId")

            if not topic_id:

                kafka_insert_topic_result = await self.fetch_one(SQL.read_sql(QueryStorage.kafka_insert_query, topic_name = topic_name))
                kafka_insert_topic_result = kafka_insert_topic_result if kafka_insert_topic_result else  {}

                topic_id = kafka_insert_topic_result.get('TopicId')

            return topic_id
        
        except Exception as e:
            print(f'Failed to process kafka topic due to - {e}')
            return None

    async def update_subreddit_schedule(self,subreddit_schedule_id, status = 'success'):

        try:

            if status.lower() == 'success': 
                await self.execute_query(SQL.read_sql(QueryStorage.update_end_subreddit_schedule_date_query, status = status, subreddit_schedule_id = subreddit_schedule_id))

            elif status.lower() == 'failed':
                await self.execute_query(SQL.read_sql(QueryStorage.update_end_subreddit_schedule_status,status = status, subreddit_schedule_id = subreddit_schedule_id))

            return True

        except Exception as e:
            raise e
    

    async def subreddit_schedule_middle_update(self, subreddit_id, subreddit_job_id, execution_type, topic_id ):

        try:
            await self.execute_query(SQL.read_sql( QueryStorage.mid_update_subreddit_job_status, subreddit_id = subreddit_id, subreddit_job_id = subreddit_job_id,  topic_id = topic_id, execution_type = execution_type))

        except Exception as e:
            print(f"Failed to execute middle subreddit schedule update  due -> {e}")

    async def insert_subreddit(self,subreddit_path):
        try:
            inserted = await self.fetch_one(SQL.read_sql(QueryStorage.insert_subreddit, subreddit_path = subreddit_path))


            return inserted.get('SubredditId') if inserted else None
        
        except Exception as e:
            print("Fail to Insert subreddit. Aborting ...")
            raise Exception
        

    async def insert_schedule(self, subreddit_id):
        try:
            inserted = await self.fetch_one(SQL.read_sql(QueryStorage.insert_subreddit_schedule, subreddit_id = subreddit_id))

            return inserted.get('SubredditScheduleId') if inserted else None

        except Exception as e:

            print("Fail to insert a schedule. Aborting ...")
            raise Exception


    async def check_for_schedule(self, sub_id, subreddit_schedule, sub_parsed_date):

        if sub_id and subreddit_schedule and sub_parsed_date  :
            return True
        
        return False


    async def execute_query(self, query, **params):
        try:
            await self.connection.execute(SQL.read_sql( query, **params))

        except Exception as e:
            print(f"Error executing query: {e}")
            

    async def fetch_data(self, query,**params):
        try:
            async with self.connection.transaction():
                async for row in self.connection.cursor(SQL.read_sql( query, **params)):
                    yield row

        except Exception as e:
            print(f"Error fetching data: {e}")

    async def fetch_one(self, query, **params):
        try:
            async with self.connection.transaction():
                row = await self.connection.fetchrow(SQL.read_sql( query, **params))
                return row
            
        except Exception as e:
            print(f"Error fetching one row: {e}")

    async def close_connection(self):
        await self.connection.close()
        print("Postgres Connection closed")

