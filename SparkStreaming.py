from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from dotenv import load_dotenv
from dataclasses import dataclass
import os

# load environment variables
load_dotenv()

@dataclass
class AppConfig:
    app_name: str
    spark_master: str
    kafka_packages: str
    es_packages: str
    kafka_broker_host: str
    kafka_topic: str
    censored_file_path: str
    window_duration: str
    censored_lit: str
    checkpoint_location: str
    es_port: str
    es_nodes: str
    es_index: str

    @staticmethod
    def from_env():
        return AppConfig(
            app_name=os.getenv('APP_NAME'),
            spark_master=os.getenv('SPARK_MASTER'),
            kafka_packages=os.getenv('KAFKA_PACKAGES'),
            es_packages=os.getenv('ES_PACKAGES'),
            kafka_broker_host=os.getenv('KAFKA_BROKER_HOST'),
            kafka_topic=os.getenv('KAFKA_TOPIC'),
            censored_file_path=os.getenv('CENSORED_FILE_PATH'),
            window_duration=os.getenv('WINDOW_DURATION'),
            censored_lit=os.getenv('CENSORED_LIT'),
            checkpoint_location=os.getenv('CHECKPOINT_LOCATION'),
            es_port=os.getenv('ES_PORT'),
            es_nodes=os.getenv('ES_NODES'),
            es_index=os.getenv('ES_INDEX')
        )

def create_spark_session(config: AppConfig):
    return SparkSession \
        .builder \
        .appName(config.app_name) \
        .master(config.spark_master) \
        .config('spark.jars.packages', f'{config.kafka_packages},{config.es_packages}') \
        .getOrCreate()

def get_kafka_stream(spark, config: AppConfig):
    return spark.readStream \
        .format('kafka') \
        .option("kafka.bootstrap.servers", config.kafka_broker_host) \
        .option("startingOffsets", "earliest") \
        .option('subscribe', config.kafka_topic).load()

def load_censored_words(file_path):
    return [x.strip() for x in open(file_path).readlines()]

def apply_censorship(df, censored_list, censored_lit):
    for word in censored_list:
        df = df.withColumn('word', when(col('word').rlike(word), lit(censored_lit)).otherwise(col('word')))
    return df

def write_to_elasticsearch(df, config: AppConfig):
    return df.writeStream \
        .outputMode("append") \
        .queryName("writing_to_es") \
        .format("org.elasticsearch.spark.sql") \
        .option("checkpointLocation", config.checkpoint_location) \
        .option("es.port", config.es_port) \
        .option("es.nodes", config.es_nodes) \
        .option("es.index.auto.create", "true") \
        .option("es.nodes.wan.only", "true") \
        .start(config.es_index)

if __name__ == "__main__":
    config = AppConfig.from_env()

    spark = create_spark_session(config)

    schema = StructType([
        StructField('speaker', StringType(), True),
        StructField('time', TimestampType(), True),
        StructField('word', StringType(), True)
    ])

    kafka_stream = get_kafka_stream(spark, config)
    kafka_stream.printSchema()

    str_df = kafka_stream.withColumn('value', kafka_stream['value'].cast(StringType()))
    fin_df = str_df.select(from_json(col('value'), schema).alias('data')).select('data.*')

    censored_list = load_censored_words(config.censored_file_path)
    censored_df = apply_censorship(fin_df, censored_list, config.censored_lit)

    censored_df.createOrReplaceTempView("Data")
    aggregated_df = censored_df \
        .withWatermark('time', config.window_duration) \
        .groupBy('speaker', window(col('time'), config.window_duration)) \
        .agg(collect_set('word').alias('words')) \
        .select('window', 'words', 'speaker')

    query = write_to_elasticsearch(aggregated_df, config)

    query.awaitTermination()

