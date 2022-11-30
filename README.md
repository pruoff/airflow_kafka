In order to run this example from within our coorporate environment, execute the following command

```
REPOSITORY=repo.bit.admin.ch:8444/ docker-compose up -d
```

docker exec --interactive --tty kafka kafka-console-producer --bootstrap-server kafka:9092 --topic TopicA
docker exec --interactive --tty kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic TopicA --from-beginning