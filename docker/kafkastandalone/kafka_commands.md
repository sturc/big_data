# Start Kafka

## Start zookeeper

`docker run -d \
--net=host \
--name=zookeeper \
-e ZOOKEEPER_CLIENT_PORT=32181 \
-e ZOOKEEPER_TICK_TIME=2000 \
-e ZOOKEEPER_SYNC_LIMIT=2 \
confluentinc/cp-zookeeper:7.0.0`

## Start kafka

```docker
docker run -d \
    --net=host \
    --name=kafka \
    -e KAFKA_ZOOKEEPER_CONNECT=localhost:32181 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:29092 \
    -e KAFKA_BROKER_ID=2 \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    confluentinc/cp-kafka:7.0.0
```

## create a topic

connect to the kafka container `docker exec -it kafka /bin/sh`

create a topic:
`kafka-topics --create --partitions 1 --replication-factor 1 --topic first-kafka-topic --bootstrap-server localhost:29092`

create events:
`kafka-console-producer --topic first-kafka-topic --bootstrap-server localhost:29092`

consume the events:
`kafka-console-consumer --topic first-kafka-topic --from-beginning --bootstrap-server localhost:29092`
