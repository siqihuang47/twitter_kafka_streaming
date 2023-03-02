# Start zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start server
bin/kafka-server-start.sh config/server.properties

# Run producer, consumer, and streaming analysis scripts
producer.py
consumer_to_db.py
streaming_analysis.py