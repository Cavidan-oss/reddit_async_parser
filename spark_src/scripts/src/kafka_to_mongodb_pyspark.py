from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, MapType, ArrayType, TimestampType, BooleanType





def connect_to_spark(app_name , master_url = "spark://spark-master", master_port = 7077 ):
    spark_session = None

    try:
        spark_session = SparkSession \
            .builder \
            .appName(app_name) \
            .master(f"{master_url}:{master_port}") \
            .config('spark.jars.packages', "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"  "org.mongodb.spark:mongo-spark-connector_2.12:10.2.1") \
            .getOrCreate()

    except Exception as e:
        print(e)

    return spark_session

def get_kafka_streaming(session, topic, kafka_server):
    df_kafka = None
    try:

        df_kafka = session \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_server) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .load() \
            .selectExpr("CAST(value AS STRING)") \
    
    except Exception as e:
        print(f'Failed to Connect Kafka - {e}')
    
    return df_kafka



def process_spark_batch(url, database, 
                            col_table_mapping = {
                                                 "Post": "Posts",
                                                 "Comment": "Comments",
                                                            }):

    def foreach_batch_function(batch_df, batch_id):
        print(f"Batch Id - {batch_id}")
        # Define type collection map
        


        # Filter batch DataFrame for posts and comments
        posts_df = batch_df.filter(col("Type") == "Post").select("Id",
                                                                        "AuthorId",
                                                                        "AuthorName",
                                                                        "Permalink",
                                                                        "Comment",
                                                                        "CreatedAt",
                                                                        "SubredditName",
                                                                        "SubredditRankingType",
                                                                        "RelatedCommentId",
                                                                        "IsActive",
                                                                        "ParsedTime"
                                                                        )

        comments_df = batch_df.filter(col("Type") == "Comment").select(
                                                                        "Id",
                                                                        "AuthorId",
                                                                        "AuthorName",
                                                                        "Permalink",
                                                                        "Comment",
                                                                        "CreatedAt",
                                                                        "SubredditName",
                                                                        "SubredditRankingType",
                                                                        "ParentPostId",
                                                                        "ParentCommentId",
                                                                        "IsActive",
                                                                        "ParsedTime"
                                                                    )


        # Write posts to MongoDB
        posts_df.write.format("mongodb") \
                    .mode("append") \
                    .option("spark.mongodb.connection.uri", url) \
                    .option("spark.mongodb.database", database) \
                    .option("spark.mongodb.collection", col_table_mapping.get("Post", "Unknown")) \
                    .save()

        # Write comments to MongoDB
        
        comments_df.write.format("mongodb") \
                    .mode("append") \
                    .option("spark.mongodb.connection.uri", url) \
                    .option("spark.mongodb.database", database) \
                    .option("spark.mongodb.collection", col_table_mapping.get("Comment", "Unknown")) \
                    .save()



        batch_df.show()

    return foreach_batch_function


def write_to_mongodb(df, database_name ,conn_url , checkpoint_location = '/temp/checkpoint'):
    
    df.writeStream \
                        .foreachBatch(process_spark_batch(url = conn_url, database = database_name)) \
                        .outputMode("append") \
                        .option("checkpointLocation", checkpoint_location) \
                        .start() \
                        .awaitTermination()



def get_structured_type():

    return StructType([
                    StructField("Type", StringType(), nullable=True),
                    StructField("Id", StringType(), nullable=True),
                    StructField("AuthorId", StringType(), nullable=True),
                    StructField("AuthorName", StringType(), nullable=True),
                    StructField("SubredditRankingType", StringType(), nullable=True),
                    StructField("Permalink", StringType(), nullable=True),
                    StructField("Comment", StringType(), nullable=True),
                    StructField("CreatedAt", StringType(), nullable=True),
                    StructField("ParentCommentId", StringType(), nullable=True),
                    StructField("ParentPostId", StringType(), nullable=True),
                    StructField("SubredditName", StringType(), nullable=True),
                    StructField("RelatedCommentId", MapType(StringType(), ArrayType(StringType())), nullable=True),
                    StructField("IsActive", BooleanType(), nullable=True),
                    StructField("ParsedTime", StringType(), nullable=False),
                    
                ])


def process_streaming_df(df, schema):

    df = df \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

    df.printSchema() 

    return df


