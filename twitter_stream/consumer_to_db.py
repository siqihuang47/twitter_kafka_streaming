#!/usr/bin/env python3
import json
from kafka import KafkaConsumer
from pymongo import MongoClient

topic_name = "twitter_kpop_data"

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='twitter-group'
)

# Connect to MongoDB and twitter_data database
try:
   client = MongoClient('localhost',27017)
   db = client.twitter_data
   print("Connected successfully!")
except:  
   print("Could not connect to MongoDB")

# connect kafka consumer to desired kafka topic	
consumer = KafkaConsumer('twitter_kpop_data', bootstrap_servers=['localhost:9092'])

for message in consumer:

    record = json.loads(message.value)
    id = record['id']
    kpop_group = record['kpop_group']

    twitter_kpop_data = {'id':id, 'kpop_group':kpop_group}