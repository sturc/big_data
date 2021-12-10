# Start Kafka

To start **kafka** execute `docker-compose up -d`

To stop **kafka** execute `docker-compose down -d`

You can connect to the kafka container with the following command: `docker exec -it kafka /bin/sh`

## Create a Topic

`kafka-topics --create --partitions 1 --replication-factor 1 --topic first-kafka-topic --bootstrap-server localhost:9092`

## Create Events for the Topic

`kafka-console-producer --topic first-kafka-topic --bootstrap-server localhost:9092`

## Consume Events of a Topic

`kafka-console-consumer --topic first-kafka-topic --from-beginning --bootstrap-server localhost:9092`
