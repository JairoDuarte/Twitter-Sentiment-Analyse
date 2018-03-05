import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import json
import twitter_config
import pykafka
import sys


class TweetListener(StreamListener):
    def __init__(self):
        # on se connecte vers le serveur kafka et instancie l'objet producer du topique twitter_samsung
        self.client = pykafka.KafkaClient("localhost:9092")
        self.producer = self.client.topics[bytes('twitter_input', 'ascii')].get_producer()

    def on_data(self, data):
        try:
            json_data = json.loads(data)
            print(json_data['text'])

            self.producer.produce(bytes(data, "ascii"))
            return True
        except KeyError:
            return True

    def on_error(self, status):
        print(status)
        return True


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: PYSPARK_PYTHON=python3 $SPARK_HOME/bin/spark-submit file.py <YOUR WORD>", file=sys.stderr)
        exit(-1)

    word = sys.argv[1]

    # connexion avec l'api twitter
    auth = OAuthHandler(twitter_config.CONSUMER_KEY, twitter_config.CONSUMER_SECRET)
    auth.set_access_token(twitter_config.ACCESS_TOKEN, twitter_config.ACCESS_TOKEN_SECRET)
    twitter_stream = Stream(auth, TweetListener())
    twitter_stream.filter(languages=['en'], track=[word])
