from src.kafka_to_mongodb_pyspark import *
import argparse
import shutil
import os


def clear_folder(folder_path):
    # Use shutil.rmtree() to remove the entire folder and its contents
    shutil.rmtree(folder_path, ignore_errors=True )
    # Recreate the empty folder
    os.makedirs(folder_path)


def main():
    parser = argparse.ArgumentParser(prog="SparkStructured Streaming  " ,
                                    description="List of available parametres",
                                    epilog="Spark Streaming between Kafka Topic and MongoDB  %(prog)s! :)")

    parser.add_argument("-n", "--name",
                        required=False ,
                        default = 'Reddit',
                        help='Name of Spark application (default -> Reddit) ')
    
    
    parser.add_argument("-ks", "--kafka_server",
                        required=False ,
                        default = 'broker:29092',
                        help='Kafka server to connect.')


    parser.add_argument("-kt", "--kafka_topic",
                        required=False ,
                        default ='Reddit',
                        help='Kafka topic to connect.')

    parser.add_argument("-md", "--mongo_database",
                        required=False ,
                        default = 'Reddit',
                        help='MongoDB Database to write a the data')
        
    parser.add_argument("-mu", "--mongo_url",
                        required=True ,
                        help='MongoDB connection string ')   
    
    parser.add_argument("-cp", "--checkpoint_path" ,
                        required=False ,
                        default = '/temp/checkpoint',
                        help='Checkpoint path for streaming') 

    parser.add_argument("-ccp", "--clear_checkpoint_path", action="store_true",
                        required=False ,
                        default = False,
                        help='Whether clear checkpoint path or not') 

    args = parser.parse_args()

    print(args)

    spark_session = connect_to_spark(args.name)

    if args.clear_checkpoint_path:
        clear_folder(args.checkpoint_path)

    if spark_session:
        df = get_kafka_streaming(spark_session, topic = args.kafka_topic, kafka_server = args.kafka_server)

        if df:
            schema = get_structured_type()

            df = process_streaming_df(df, schema)


            write_to_mongodb(df,database_name =  args.mongo_database, conn_url = args.mongo_url, checkpoint_location = args.checkpoint_path)


if __name__ == '__main__':

    main()

    # docker exec -it 304f0c1da73d  bash -c "cd  && python temp/scripts/streaming_main.py -mu 'mongodb://host.docker.internal:27017' -ccp"
    
