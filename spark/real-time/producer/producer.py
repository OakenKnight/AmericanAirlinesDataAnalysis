#!/usr/bin/python3

import os
import time
import json
from tweepy import OAuthHandler
from tweepy import Stream

from kafka import KafkaProducer
import kafka.errors

from decouple import config

KAFKA_BROKER=config("KAFKA_BROKER")

API_KEY = config('TWITTER_API_KEY')
API_SECRET = config('TWITTER_API_SECRET')
ACCESS_TOKEN = config("ACCESS_TOKEN")
ACCESS_SECRET = config("ACCESS_SECRET")

topic_name = "airplane-delays-topic"

while True:
    try:
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER.split(","))
        print("Connected to Kafka!")
        break
    except kafka.errors.NoBrokersAvailable as e:
        print(e)
        time.sleep(3)


class ListenerTS(Stream):

    def on_status(self, status):
        if hasattr(status, "extended_tweet"):
            text = status.extended_tweet['full_text']
        else:
            text = status.text
        user = status.user.screen_name
        value = {'text': text, 'user': user}
        value_json = json.dumps(value)
        print(value_json)
        # producer.send(topic_name, bytes(value_json, 'utf-8'))
        return True

print("\n\n\n+++++++ PRODUCER STARTED +++++++\n\n\n")

listener = ListenerTS(API_KEY, API_SECRET, ACCESS_TOKEN, ACCESS_SECRET)
listener.filter(track=["flight delayed", "flight delay", "flights delayed", "flight canceled","flight cancelled","flights canceled", "flights cancelled"], \
    stall_warnings=True, languages=["en"])
