docker exec --interactive --tty kafka kafka-console-producer --bootstrap-server kafka:9092 --topic TopicA


docker exec --interactive --tty kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic TopicA --from-beginning