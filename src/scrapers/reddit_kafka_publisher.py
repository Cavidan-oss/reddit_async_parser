import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/..")
import pytz
from scrapers.reddit_parser import RedditScraper
from utils.kafka_producer import AsyncKafkaProducer
from utils.postgres_helper import AsyncPostgreSQLHelper
from utils.decorators import log_to_postgres, async_provide_postgres_connection, async_provide_kafka_connection
import asyncio
from dotenv import dotenv_values
from datetime import datetime, timedelta
from config import Config

env_vars = dotenv_values(".env")


class RedditKafkaProducer(RedditScraper):

    AVAILABLE_PATHS = {
        'tophour' : 'top/?sort=top&t=hour',
        'day': 'top/?sort=top&t=day',
        'topweek' : 'top/?sort=top&t=week',
        'topmonth' : 'top/?sort=top&t=month',
        'topyear' : 'top/?sort=top&t=year' , 
        'topall' : 'top/?sort=top&t=all' ,
        'new' : 'new/' ,
        'hot' :'/',
        'rising' : '/rising/',
        'controversial' : 'controversial/'
    } 

    def __init__(self, max_tabs = 15, user_agent = None) -> None:

        if not user_agent:
            user_agent = Config.get(
                'WEB_USER_AGENT'
            )

        super().__init__(max_concurrent_tabs=max_tabs, user_agent= user_agent)
        self.event_loop = asyncio.get_event_loop()

        self.kafka_producer = None
        self.postgres_helper = None

        self._kafka_connection_status = False
        self._postgres_connection_status = False

        self.user_agent  = user_agent


    def get_modified_subreddit_path(self,subreddit_path, path_to_add = 'new'):
        user_path = subreddit_path.strip('/')
        user_path = f'/{user_path}/'

        if path_to_add in user_path or path_to_add not in RedditKafkaProducer.AVAILABLE_PATHS.keys():
            return user_path
        else:
            return user_path + RedditKafkaProducer.AVAILABLE_PATHS.get(path_to_add, 'new/')

    
    @staticmethod
    def get_human_readable_time(seconds):
        # Calculate hours, minutes, and remaining seconds
        hours, remainder = divmod(seconds, 3600)
        minutes, seconds = divmod(remainder, 60)

        # Format the result
        result = "{:02}:{:02}:{:02}".format(int(hours), int(minutes), int(seconds))
        return result
    
    @log_to_postgres()
    @async_provide_kafka_connection()
    async def subreddit_kafka_producer(self, subreddit, parse_all = False, subreddit_type = 'new' , count_limit  = None,  kafka_topic = 'Reddit', period = None, direct_subreddit_path = False, close_browser = True, execution_type = 'manual', *args, **kwargs):
        
        try:
            self.kafka_producer = kwargs.get('kafka_session')
            self.postgres_helper = kwargs.get('postgres_session')

            subreddit_job_id = kwargs.get('subreddit_job_id')
            subreddit_id, subreddit_schedule_id, last_parsed_date = await self.postgres_helper.process_subreddit_conf(subreddit)

            kafka_topic_id = await self.postgres_helper.process_kafka_topic(kafka_topic)
            
            await self.postgres_helper.subreddit_schedule_middle_update( subreddit_id = subreddit_id,subreddit_job_id = subreddit_job_id,  topic_id = kafka_topic_id, execution_type = execution_type) 

            subreddit, parse_all, period, subreddit_type, count_limit = await self.prepare_params_for_parsing(period=period, parse_all=parse_all, last_parsed_date= last_parsed_date, direct_subreddit_path=direct_subreddit_path, subreddit=subreddit, subreddit_type= subreddit_type, count_limit = count_limit)
            
            await self.parse_and_publish(subreddit, kafka_topic, parse_all, period, count_limit, subreddit_type, close_browser)

        except Exception as e:

            await self.postgres_helper.update_subreddit_schedule(subreddit_schedule_id = subreddit_schedule_id, status='failed')
            print(f"Error Occured - {e}")

            raise Exception(e)

        else:
            await self.postgres_helper.update_subreddit_schedule(subreddit_schedule_id = subreddit_schedule_id)



    async def prepare_params_for_parsing(self, period, parse_all, last_parsed_date, direct_subreddit_path, subreddit, subreddit_type, count_limit):


        if not period and last_parsed_date:
                
            datetime_now = datetime.now()
            new_timezone = pytz.timezone('Asia/Baku')

            last_parsed_date = last_parsed_date.astimezone(new_timezone)
            datetime_now = datetime_now.astimezone(new_timezone)
            period  =  (datetime_now-( last_parsed_date) ).total_seconds()
    

        if not period and not last_parsed_date and not count_limit:
            parse_all = True

        if parse_all:
            print('Whole subreddit will be parsed')
        
        elif count_limit:
            print(f'Count Limit detected. Only {count_limit} posts will be parsed.')

        elif  period:
            print(f"Posts up to this time will be searched : {RedditKafkaProducer.get_human_readable_time(period)}")


        if not direct_subreddit_path:
            subreddit = self.get_modified_subreddit_path(subreddit, subreddit_type)

        
        return (subreddit, parse_all, period, subreddit_type, count_limit)


    async def parse_and_publish(self, subreddit_path, kafka_topic, parse_all, period, count_limit, subreddit_type, close_browser):

        async for result in self.parse_subreddit(subreddit_path, parse_all, count_limit, period=period, close_visual_browser = close_browser):
            # Process each result as it becomes available
            for post_data in result:
                post_data.update({
                                  'SubredditName': subreddit_path,
                                  "SubredditRankingType" :subreddit_type,
                                  "ParsedTime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                  "IsActive" : True}
                                  )
                await self.kafka_producer.push_to_kafka(kafka_topic, post_data)     



if __name__ == '__main__':

    producer =  RedditKafkaProducer()

    asyncio.run(producer.subreddit_kafka_producer('r/GradSchool/', subreddit_type = 'topall', parse_all=True , close_browser=True ))

    