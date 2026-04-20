import logging
import uuid
from datetime import datetime, timedelta

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streaming
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    logging.info("Keyspace created successfully")

def create_tables(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streaming.user_created (
            user_id text PRIMARY KEY,
            name text,
            age int,
            gender text,
            email text,
            phone text,
            cell text,
            id text,
            picture text,
            nat text,
            registered_date text
        )
    """)
    logging.info("Table created successfully")

def insert_data(session, data):
    print(f"Inserting data: {data}")
    try:
        session.execute("""
            INSERT INTO spark_streaming.user_created (
                user_id,
                name,
                age,
                gender,
                email,
                phone,
                cell,
                id,
                picture,
                nat,
                registered_date
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (str(uuid.uuid4()),
            data['name'], 
            data['age'], 
            data['gender'], 
            data['email'], 
            data['phone'], 
            data['cell'], 
            data['id'], 
            data['picture'], 
            data['nat'], 
            data['registered_date']
        ))

        logging.info("Data inserted successfully")
    except Exception as e:
        logging.error(f"Error inserting data: {e}")

def create_spark_connection():
    try:
        spark = SparkSession.builder\
            .appName("SparkStreaming")\
            .config(
                "spark.jars.packages", 
                "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
            )\
            .config("spark.cassandra.connection.host", "localhost")\
            .config("spark.cassandra.auth.username", "admin")\
            .config("spark.cassandra.auth.password", "admin")\
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully")
        return spark
    except Exception as e:
        logging.error(f"Error creating spark connection: {e}")
        return None

def connect_to_kafka(spark):
    try:
        data_stream = spark.readStream.format("kafka")\
            .option("kafka.bootstrap.servers", "localhost:9092")\
            .option("subscribe", "user_created")\
            .option("startingOffsets", "earliest")\
            .load()
        logging.info("Kafka connection created successfully")
        return data_stream
    except Exception as e:
        logging.error(f"Error connecting to kafka: {e}")
        return None

def create_cassandra_connection():
    try:
        cluster = Cluster(["localhost"], auth_provider=PlainTextAuthProvider("admin", "admin"))
        session = cluster.connect()
        logging.info("Cassandra connection created successfully")
        return session
    except Exception as e:
        logging.error(f"Error creating cassandra connection: {e}")
        return None

def create_selection_df_from_kafka(data_stream):
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("gender", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("cell", StringType(), True),
        StructField("id", StringType(), True),
        StructField("picture", StringType(), True),
        StructField("nat", StringType(), True),
        StructField("registered_date", StringType(), True),
    ])
    try:
        data_stream = data_stream.selectExpr("CAST(value AS STRING)")
        data_stream = data_stream.select(from_json(col("value"), schema).alias("data"))
        data_stream = data_stream.select("data.*")
        logging.info("Data transformed successfully")
        return data_stream
    except Exception as e:
        logging.error(f"Error transforming data: {e}")
        return None

if __name__ == '__main__':
    spark = create_spark_connection()
    
    if spark is not None:
        cassandra_session = create_cassandra_connection()
        data_stream = connect_to_kafka(spark)
        if cassandra_session is not None:
            create_keyspace(cassandra_session)
            create_tables(cassandra_session)
            insert_data(cassandra_session)
            