#!/bin/bash
# Creates all topics. 3 partitions so consumers in a group can scale out; keyed by orderId so one order's events stay ordered.
BS=kafka:9092
for t in orders.created payments.completed payments.failed order.status delivery.assigned delivery.completed; do
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server $BS --create --if-not-exists --topic $t --partitions 3 --replication-factor 1
done
/opt/kafka/bin/kafka-topics.sh --bootstrap-server $BS --list
