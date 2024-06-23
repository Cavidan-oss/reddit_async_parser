import argparse
from scrapers.reddit_kafka_publisher import RedditKafkaProducer
import asyncio
from config import Config

def main():
    parser = argparse.ArgumentParser(prog="RedditParserCLI" ,
                                    description="List of available parametres",
                                    epilog="Thanks for using %(prog)s! :)")


    parser.add_argument("-n", "--name",
                        required=True ,
                        help='Name of Subreddit to parse eg : (r/gradadmissions/) ')


    parser.add_argument("-t", "--type",
                        required=False ,
                        default= 'new',
                        choices = ['tophour', 'day', 'topweek', 'topmonth', 'topyear', 'topall', 'new', 'hot', 'rising', 'controversial'],
                        help='In which order type subreddit has to be parsed. ')

    parser.add_argument("-mt", "--max_tabs",
                        type=int,  
                        default=15, 
                        required=False ,
                        help='Maximum amount of tabs that can be opened. More tabs more RAM usage less parsing time')

    parser.add_argument("-a", "--all", action="store_true",
                        default=False, 
                        required=False ,
                        help='Parse the entire subreddit (default: False)')

    parser.add_argument("-l", "--limit",
                        type=int,  
                        default=False, 
                        required=False ,
                        help='Maximum amount of posts that processed. If given time period will be ignored!')

    parser.add_argument("-p", "--period",
                        required=False ,
                        default= None,
                        help='Time span to scrape the posts. Used for scraping the data from /new ordering type. If not given (assuming limit also not given) app will get the last parsed date from database. ')


    parser.add_argument("-dn", "--direct_name", action="store_true",
                        default=False, 
                        required=False ,
                        help="Indicating user entered full subreddit path and don't need subreddit modification.")


    parser.add_argument("-ob", "--open_browser", action="store_false",
                        default=True, 
                        required=False ,
                        help="Whether open up browser instance visually to parse or not. (True - > Do not open | False -> Open browser)")


    parser.add_argument("-kt", "--kafka_topic",
                        required=False ,
                        default=Config.get('DEFAULT_KAFKA_TOPIC'),
                        help='Kafka topic to publish the results into')

    
    args = parser.parse_args()

    producer =  RedditKafkaProducer(max_tabs= args.max_tabs)

    print(args)

    asyncio.run(producer.subreddit_kafka_producer(subreddit=args.name
                                                  , subreddit_type = args.type 
                                                  , parse_all = args.all
                                                  , kafka_topic= args.kafka_topic
                                                  , direct_subreddit_path= args.direct_name
                                                  , count_limit=args.limit
                                                  , close_browser=args.open_browser
                                                  , period=args.period ))



if __name__ == '__main__':
    main()