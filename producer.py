import os
from kafka import KafkaProducer
import json
from datetime import datetime, timedelta
import time
import random
from dotenv import load_dotenv

# load environment variables
load_dotenv()

class KafkaProducerService:
    def __init__(self, brokers, topic):
        self.producer = KafkaProducer(
            bootstrap_servers=brokers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            api_version=(0, 11, 5)
        )
        self.topic = topic

    def send_message(self, message):
        self.producer.send(self.topic, value=message)


class MessageGenerator:
    def __init__(self, speakers, dictionary_path):
        self.speakers = speakers
        self.dictionary_path = dictionary_path
        self.first_time = datetime.now()

    def _load_words(self):
        with open(self.dictionary_path, 'r') as file:
            return [line.strip() for line in file.readlines()]

    def generate_initial_messages(self):
        words = self._load_words()
        speaker_range = self.speakers[:]
        messages = []
        for _ in range(len(self.speakers)):
            speaker = random.choice(speaker_range)
            message = {
                'speaker': speaker,
                'time': str(self.first_time),
                'word': random.choice(words)
            }
            speaker_range.remove(speaker)
            messages.append(message)
        return messages

    def generate_continuous_message(self, iteration):
        words = self._load_words()
        return {
            'speaker': random.choice(self.speakers),
            'time': str(self.first_time + timedelta(seconds=10 * (iteration + 1))),
            'word': random.choice(words)
        }


class ProducerApp:
    def __init__(self, kafka_service, message_generator):
        self.kafka_service = kafka_service
        self.message_generator = message_generator

    def run(self):
        print('Kafka Producer has been initiated...')
        # send initial messages
        initial_messages = self.message_generator.generate_initial_messages()
        for message in initial_messages:
            print(message)
            self.kafka_service.send_message(json.dumps(message))
            time.sleep(3)

        # send continuous messages
        for i in range(9999):
            message = self.message_generator.generate_continuous_message(i)
            print(message)
            self.kafka_service.send_message(json.dumps(message))
            time.sleep(3)


if __name__ == "__main__":
    brokers = os.getenv('BROKERS').split(',')
    kafka_topic = os.getenv('KAFKA_TOPIC')
    speakers = os.getenv('SPEAKERS').split(',')
    dictionary_path = os.getenv('DICTIONARY_PATH')

    kafka_service = KafkaProducerService(brokers, kafka_topic)
    message_generator = MessageGenerator(speakers, dictionary_path)
    app = ProducerApp(kafka_service, message_generator)
    app.run()


