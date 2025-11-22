# README

Project is demonstrating Kafka setup using Kafka KRaft architecture.

Kafka 3.3+: KRaft is production-ready for new clusters. Kafka 3.5+: KRaft is the default metadata mode. Kafka 4.0 and beyond: ZooKeeper will be fully removed, and only KRaft will be supported. (from Googe search; 3.3+ confirmed on Confluent website).


Kraft mode removes the need for ZooKeeper, simplifying the architecture and management of Kafka clusters. Documentation:
https://developer.confluent.io/learn/kraft/


## Docker (November 2025)
Docker compose file is based on the latest Confluent instructions as of November 2025.
https://developer.confluent.io/confluent-tutorials/kafka-on-docker/

Currently using Kafka version 4.1.1.

### UI
UI is built on top of two options
- Provectus: https://github.com/provectus/kafka-ui
- Kafdrop: https://github.com/obsidiandynamics/kafdrop
https://kafdrop.com/

Provectus seems to be less mainintained recently, lastly in 2024 but it has more features.

### Previous example project 
Previous example project with ZooKeeper based Kafka cluster including schema registry, Kafka Connect and UI:
https://github.com/mmaracic/kafka-long-running-process/blob/master/docker-compose.yml
