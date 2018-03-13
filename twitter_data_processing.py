"""
  Created by Jairo Duarte on 22/02/2018.
"""

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pymongo import MongoClient
import json
import pykafka
from afinn import Afinn


# retourne le status d'un tweet
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


# persiste les infos d'un tweet dans la base de données
def send_data(text):
    try:
        connection = MongoClient()
        db = connection.twitter_db
        coll_tweets = db.tweets

        data = '{}'
        json_send_data = json.loads(data)
        json_send_data['text'] = text['text']
        # récupère la valeur de sentiment d’un tweet avec la fonction score() et  l’affecte dans la variable  à persister
        json_send_data['sentiment_value'] = afinn.score(text['text'])
        json_send_data['status'] = fun(json_send_data['sentiment_value'])
        json_send_data['location'] = text['user']['location']
        if json_send_data['location'] == "null" or json_send_data['location'] == "None":
            json_send_data['location'] = ""

        print(json_send_data['text'], ">>>", json_send_data['sentiment_value'], ">>>", json_send_data['status'])
        coll_tweets.insert_one(json_send_data)
        connection.close()
        return json_send_data['status']
    except Exception as inst:
        print(inst)
        return None


# persiste les status dans la BD avec la quantité des tweets pour chaque classe
def send_status(partition):
    try:
        connection = MongoClient()
        db = connection.twitter_db
        col_status = db.status
        for tup in partition:
            key = tup[0]
            value = tup[1]
            col_status.update({"_id": key}, {"$inc": {"count": value}}, upsert=True)
            print(key, "---%--", value)
        connection.close()
    except Exception as inst:
        print(inst)


if __name__ == "__main__":
    # création d'un Spark context pour connecter un cluster Spark
    sc = SparkContext(appName="TwitterSentimentAnalyse")
    afinn = Afinn()

    # définir l'intervalle de traitement en batch
    ssc = StreamingContext(sc, 10)

    # création d'un Kafka Stream pour consommer des données du topique twitter_input
    kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {'twitter_input': 1})

    # convertir les données twitter en json
    parsed = kafkaStream.map(lambda v: json.loads(v[1]))

    # compter chaque status en utilisant map, retourne clè/valeur (status/1)
    # reduceByKey fait la somme pour toutes valeurs qui ont le même status
    status_counts = parsed.map(
        lambda tweet: (send_data(tweet), 1)).reduceByKey(lambda x, y: x + y)
    # parcourir chaque element du RDD et l’envoyer dans la fonction send_status()
    status_counts.foreachRDD(lambda rdd: rdd.foreachPartition(send_status))

    # afficher le nombre de tweet pour status
    print(status_counts.pprint())

    # Start execution du streaming
    ssc.start()
    ssc.awaitTermination()
