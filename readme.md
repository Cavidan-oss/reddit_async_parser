# Reddit Asynchronous Parser
A project designed to parse Reddit subreddit pages asynchronously and provide a convenient way to store the retrieved data for future use


![Reddit Parser Architecture](https://github.com/Cavidan-oss/reddit_async_parser/blob/7feb3a884166e58f5e89bed8b00dcedf70353dc7/documentation/RedditArchitecture.png)



## Project Description

The Reddit Asynchronous Parser is an application built to efficiently parse Reddit subreddit pages using the async version of the Playwright library. The retrieved data is then pushed into a specified Kafka topic. On the other end of the Kafka topic, an Apache Spark Structured Streaming process is triggered. After undergoing a few modifications, the data is split into two different groups, namely Posts and Comments, and then pushed into the specified MongoDB instance. The Reddit Asynchronous Parser is intelligently designed to utilize a PostgreSQL instance to oversee the entire process. This integration allows for efficient utilization of previous parsing results, enabling seamless continuation of parsing processes with enhanced efficiency and accuracy. Leveraging an asynchronous model, both MongoDB and PostgreSQL are utilized to manage data operations effectively, ensuring smooth operation and reliability of the application.

In order to interact with the parsing and streaming processes, there are two command-line interfaces available. All the components, except the application itself, are deployed inside Docker containers. This deployment method ensures scalability and ease of management, although it presents some challenges during the interconnection of components. 


### Features

- Asynchronous parsing of Reddit subreddit pages
- Integration with Kafka for data streaming
- Utilization of Apache Spark Structured Streaming for real-time data processing
- Integration with PostgreSQL for comprehensive process control and history-driven decision-making
- Storage of parsed data in MongoDB
- Ready to use data storing architecture for further usage of human generated data
- Docker containerization for easy deployment and management
- Command-line interfaces for interaction with parsing and streaming processes


## Installation

To install the Reddit Asynchronous Parser, comprehensive installation process needed. Follow these steps below:

#### 1. Copying repo
Clone the repository from GitHub.
```
https://github.com/Cavidan-oss/reddit_async_parser.git
```
#### 2. Navigating
Navigate to the project directory.
```
cd reddit_parser
```

#### 3. Setting up virtual environment (Optional)
To ensure a clean installation and prevent library collisions, it's recommended to set up a virtual Python environment. If you haven't already, you can install the virtualenv library using pip:
```bash
pip install virtualenv
```

Next, create and activate the virtual environment:
```bash
python -m venv env
env\Scripts\activate  # For Windows
source env/bin/activate  # For macOS/Linux
```
#### 4. Installing needed packages
Once activated, you can download the required Python packages and libraries specified in the `requirements.txt` file:
```bash
pip install -r requirements.txt
```
After installing the required packages, you'll also need to set up the Playwright web driver. You can do this by executing the following command:
```bash
playwright install
```

#### 5. Docker Installation and Application Deployment

Ensure you have Docker Engine installed. If not, refer to the [official Docker installation guide](https://docs.docker.com/engine/install/).

Before building the Docker images, you need to specify the configuration inside the `.env` file. This file includes several fields for specifying usernames, passwords, database storage paths, and endpoints. Do not change the keywords, as they are internally bound to keys. Here is an example version of a ready `.env` file. On the app-related part include your web user agent to be able to parse the subreddits without creating browser instance visually.From my experience, you can run the application without "WEB_USER_AGENT" by triggering in open-browser mode, but it causes a forbidden error in closed-browser mode.
> [!IMPORTANT]
> It is advised not to change the ports and leave them as in the example.

```bash
# Kafka Credentials
DEFAULT_KAFKA_TOPIC=Reddit

KAFKA_EXTERNAL_HOST=localhost
KAFKA_EXTERNAL_PORT=9092

KAFKA_INSIDE_HOST=broker
KAFKA_INSIDE_PORT=29092

# MongoDB Credentials
MONGO_HOST=host.docker.internal
MONGO_USERNAME=  # Not Using
MONGO_PASSWORD=  # Not Using
MONGO_DATABASE=Reddit
MONGO_PORT=27017

MONGO_DATA_STORAGE_PATH=/c/path/to/storage  # Absolute path which will be mounted

# PostgreSQL Credentials
POSTGRES_HOST=localhost
POSTGRES_PORT=6432
POSTGRES_DATABASE=RedditParser
POSTGRES_REDDIT_USERNAME=Admin
POSTGRES_REDDIT_PASSWORD=admin

POSTGRES_DATA_STORAGE_PATH=/c/path/to/storage  # Absolute path which will be mounted

# App Related

WEB_USER_AGENT = 


```
Firstly we need to create a network named Reddit for. It can be created using command below:



```bash
docker network create Reddit 
```

Next, create a custom image for Apache Spark, which includes additional libraries. Build the image using the following commands. Ensure to tag it as custom_spark_app, as it is used inside the docker-compose.yaml file:

```bash
docker build . -f ./infrastructure/Apache_Spark.Dockerfile -t custom_spark_app 
```

After successfully creating the custom image, deploy the application services using the following command:

``` bash
docker-compose -f infrastructure\all_docker_compose.yaml   --env-file .env up -d  
```

#### 6.  Finalzing the deployment. 
To finalize the deployment, set up the PostgreSQL and Kafka structures by executing the following command:

``` bash
python infrastructure\deploy_database.py

```

## Usage


Once installed, you can utilize the command-line interfaces to interact with the parsing and streaming processes. Below are some fundamental usage examples. The application comprises two distinct parts that can be accessed through the command-line interface.

To interact with the parser, navigate to the file located at src/scrapers/reddit_kafka_publisher.py. However, to directly access the Spark application, users must enter the Spark master environment. This environment includes three mounted volumes: scripts, spark_apps, and spark_data. While spark_apps and spark_data are system-critical and located within the /opt folder, scripts is mounted into the /temp folder. To trigger the Spark application, users can execute spark_src/spark_main.py, which initiates the Spark application using docker exec along with additional parameters. Alternatively, if users wish to provide specific details directly, they can execute spark_src/scripts/streaming_main.py alongside the docker exec command.


``` bash
python spark_src/spark_main.py

```
This process initiates by downloading the necessary artifacts and proceeds to create structured streaming processes to move the data into MongoDB. It is important to note that the Spark instance can only access the data from its starting point. Therefore, users should start the Spark application first before proceeding with parsing subreddits.



![Spark Interface Example](https://github.com/Cavidan-oss/reddit_async_parser/blob/7feb3a884166e58f5e89bed8b00dcedf70353dc7/documentation/spark_interface.png)

To initiate the application, utilize the command-line interface provided by the main function within the /src folder. The provided script scrapes the GRE subreddit and retrieves data from a latest single post, as indicated by the -l 1 flag. Feel free to explore different flags by indicating --help flag.


``` bash
python src\main.py -n r/GRE/ -l 1

```

![Command Line Interface Parser Example](https://github.com/Cavidan-oss/reddit_async_parser/blob/7feb3a884166e58f5e89bed8b00dcedf70353dc7/documentation/result_of_parser_command_line.png)


 
To verify the parsed data, navigate to the control center (defaulted to localhost:9021) under the topic and messages section. However, to push the retrieved data into the appropriate MongoDB collections for storage, the Apache Spark instance must be triggered. For user convenience, triggering spark_src/spark_main.py suffices.

![Control Center Example](https://github.com/Cavidan-oss/reddit_async_parser/blob/7feb3a884166e58f5e89bed8b00dcedf70353dc7/documentation/control_center_image.png)





## Contributing 
Contributions to the Reddit Asynchronous Parser project are welcome! 

## License
This project is licensed under the [MIT](license.txt) license. Feel free to use, modify, and distribute this code for any purpose.
