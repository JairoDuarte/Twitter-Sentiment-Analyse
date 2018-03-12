"""
RUNNING PROGRAM;

1-Start Apache Kafka
./kafka/kafka_2.11-0.11.0.0/bin/kafka-server-start.sh ./kafka/kafka_2.11-0.11.0.0/config/server.properties

2-Run kafka_push_listener.py (Start Producer)
ipython >> run kafka_push_listener.py

3-Run kafka_twitter_spark_streaming.py (Start Consumer)
PYSPARK_PYTHON=python3 bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 kafka_twitter_spark_streaming.py

"""

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pymongo import MongoClient
import json
import pykafka
from afinn import Afinn


def fun(senti_val):
    try:
        if senti_val < 0:
            return 'NEGATIVE'
        elif senti_val == 0:
            return 'NEUTRAL'
        else:
            return 'POSITIVE'
    except TypeError:
        return 'NEUTRAL'


def send_data(text):
    connection = MongoClient('mongodb://admin:dba@ds012178.mlab.com:12178/twitter_db')
    db = connection.twitter_db
    coll_tweets = db.tweets

    data = '{}'
    json_send_data = json.loads(data)
    json_send_data['text'] = text['text']
    json_send_data['sentiment_value'] = afinn.score(text['text'])
    json_send_data['status'] = fun(json_send_data['sentiment_value'])
    json_send_data['location'] = text['user']['location']
    if json_send_data['location'] == "null":
        json_send_data['location'] = ""

    print(json_send_data['text'], ">>>>>>>>>>", json_send_data['sentiment_value'], ">>>>>>>>>>",
          json_send_data['status'])
    coll_tweets.insert_one(json_send_data)
    # coll_tweets.update(json_send_data)
    print(json_send_data)
    connection.close()
    return json_send_data['status']


def send_status(partition):
    connection = MongoClient('mongodb://admin:dba@ds012178.mlab.com:12178/twitter_db')
    db = connection.twitter_db
    col_status = db.status
    for tup in partition:
        key = tup[0]
        value = tup[1]
        col_status.update({"_id": key}, {"$inc": {"count": value}}, upsert=True)
        print(key, "-----", value)
    connection.close()


if __name__ == "__main__":
    # Create Spark Context to Connect Spark Cluster
    sc = SparkContext(appName="TwitterSentimentAnalyse")
    afinn = Afinn()

    # Set the Batch Interval is 10 sec of Streaming Context
    ssc = StreamingContext(sc, 10)

    # Create Kafka Stream to Consume Data Comes From Twitter Topic
    # localhost:2181 = Default Zookeeper Consumer Address
    kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {'twitter_input': 1})

    # Parse Twitter Data as json
    parsed = kafkaStream.map(lambda v: json.loads(v[1]))

    # Count the number of tweets per User
    user_counts = parsed.map(
        lambda tweet: (send_data(tweet), 1)).reduceByKey(lambda x, y: x + y)
    user_counts.foreachRDD(lambda rdd: rdd.foreachPartition(send_status))

    # Print the User tweet counts
    print(user_counts.pprint())

    # Start Execution of Streams
    ssc.start()
    ssc.awaitTermination()
