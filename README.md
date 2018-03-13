#Readme 

Pour executer le programme:

1- Start zookepper
zookeeper-server-start.sh  $KAFKA_HOME/config/zookeeper.properties

2-Start Apache Kafka
kafka-server-start.sh $KAFKA_HOME/config/server.properties

3-Run twitter_data_ingestion.py (Start Producer)
PYSPARK_PYTHON=python3 spark-submit twitter_data_ingestion.py "Donald Trump"

4-Run twitter_data_processing.py (Start Consumer)
PYSPARK_PYTHON=python3 spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 twitter_data_processing.py
PYSPARK_PYTHON=python3 spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0  twitter_data_processing.py