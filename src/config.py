from dotenv import load_dotenv, find_dotenv
import os



load_dotenv(find_dotenv())


class Config:
  __conf = {
    "POSTGRES_USER" : os.environ.get( 'POSTGRES_REDDIT_USERNAME' ),
    "POSTGRES_PASSWORD" : os.environ.get('POSTGRES_REDDIT_PASSWORD'),
    "POSTGRES_HOST" : os.environ.get('POSTGRES_HOST'),
    "POSTGRES_PORT" : os.environ.get('POSTGRES_PORT'),
    "POSTGRES_DATABASE": os.environ.get('POSTGRES_DATABASE') ,
    "KAFKA_HOST" : os.environ.get('KAFKA_EXTERNAL_HOST') , 
    "KAFKA_PORT" : os.environ.get('KAFKA_EXTERNAL_PORT') ,
    "DEFAULT_KAFKA_TOPIC" : os.environ.get('DEFAULT_KAFKA_TOPIC'),
    "KAFKA_INSIDE_HOST" : os.environ.get('KAFKA_INSIDE_HOST'),
    "KAFKA_INSIDE_PORT" : os.environ.get('KAFKA_INSIDE_PORT'),
    "MONGO_HOST" : os.environ.get('MONGO_HOST') ,
    "MONGO_PORT" : os.environ.get("MONGO_PORT") , 
    "MONGO_USERNAME" : os.environ.get('MONGO_USERNAME') ,
    "MONGO_PASSWORD" : os.environ.get('MONGO_PASSWORD') ,
    "MONGO_DATABASE" : os.environ.get('MONGO_DATABASE') ,
    "WEB_USER_AGENT" : os.environ.get('WEB_USER_AGENT')
  }
  __setters = []

  @staticmethod
  def get(name):
    return Config.__conf.get(name)

  @staticmethod
  def set(name, value):
    if name in Config.__setters:
      Config.__conf[name] = value
    else:
      raise NameError("Name not accepted in set() method")
  