# A project to quickly start up Spark/Spark Streaming development

## Pre-requisite
1. JDK 1.7.x+ (1.7.0_45)
2. SBT 0.13.x (0.13.5)

## To run
1. sbt clean package
2. sbt run, then pick the class you want to run from the list.

or
1. Using spark submit.
bin/spark-submit \
--class "streaming.KafkaApp1" \
--master local[2] \
target/scala-2.10/sparklearning-assembly-1.0.jar \
KafkaIntegrationTest dnvrco01-os-coe0002.conops.timewarnercable.com:9092 gemini_v01 10

Test1
Test2
