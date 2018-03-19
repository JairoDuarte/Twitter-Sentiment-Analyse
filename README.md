### Pour executer le programme:

1- Start zookepper
zookeeper-server-start.sh  $KAFKA_HOME/config/zookeeper.properties

2-Start Apache Kafka
kafka-server-start.sh $KAFKA_HOME/config/server.properties

3-Run twitter_data_ingestion.py (Start Producer)
PYSPARK_PYTHON=python3 spark-submit twitter_data_ingestion.py "Donald Trump"

4-Run twitter_data_processing.py (Start Consumer)
PYSPARK_PYTHON=python3 spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0  twitter_data_processing.py

<img width="1440" alt="1" src="https://user-images.githubusercontent.com/16276182/37610932-dbce60b0-2b98-11e8-9a05-78f1e45ba2d1.png">

### Visualisation Dashboard:

<img width="1440" alt="2" src="https://user-images.githubusercontent.com/16276182/37610964-febb10be-2b98-11e8-9a2b-545490d99d1d.png">

