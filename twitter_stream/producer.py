#!/usr/bin/env python3
from kafka import KafkaProducer
import tweepy
from tweepy import OAuthHandler
import json
import config

topic_name = 'twitter_stream'
search_tags = ['kpop']
producer = KafkaProducer(bootstrap_servers='localhost:9092')


class KafkaPushListener(tweepy.StreamingClient):
    """ A class to read the twitter stream and push it to Kafka"""
    def on_data(self, raw_data):
        producer.send(topic_name, raw_data)
        print(raw_data)
        return True

    def on_error(self, status):
        print(status)
        return True

if __name__ == "__main__":
    auth = OAuthHandler(config.api_key, config.api_key_secret)
    auth.set_access_token(config.access_token, config.access_token_secret)
    api = tweepy.API(auth)

    streaming_client = KafkaPushListener(config.bearer_token_key)
   
    # Add rules for tag to be included.
    for tag in search_tags:
        streaming_client.add_rules(tweepy.StreamRule(tag))
    
    streaming_client.delete_rules(['1631122042778189824', '1631127679629152257','1631127779290021889','1631127779797524482'])

    print(streaming_client.get_rules())

    streaming_client.disconnect()
    print(streaming_client.running)
    #streaming_client.filter()
