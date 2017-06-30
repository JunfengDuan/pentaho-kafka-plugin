pentaho-kafka-producer
======================

Apache Kafka producer step plug-in for Pentaho Kettle.

[![Build Status](https://travis-ci.org/RuckusWirelessIL/pentaho-kafka-producer.png)](https://travis-ci.org/RuckusWirelessIL/pentaho-kafka-producer)


### Screenshots ###

![Using Apache Kafka Producer in Kettle](https://github.com/JunfengDuan/kettle-kafka-plugin/blob/master/doc/example1.png)

### Pentaho Compatibility ###

The kettle version is 7.0.0.0-25

### Apache Kafka Compatibility ###

The producer depends on Apache Kafka 0.10.2.0, which means that the broker must be of 0.10.x version or later.


### Installation ###

1. Download ```pentaho-kafka-producer``` Zip archive from [latest release page](https://github.com/RuckusWirelessIL/pentaho-kafka-producer/releases/latest).
2. Extract downloaded archive into *plugins/steps* directory of your Pentaho Data Integration distribution.


### Building from source code ###

```
mvn clean package
```
