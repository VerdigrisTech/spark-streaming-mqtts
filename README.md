# spark-streaming-mqtts [![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/VerdigrisTech/spec-pkcs1/blob/master/LICENSE.md)

This module adds client side SSL certificate and TLS v1.2 support to the
`MQTTUtils` class provided by [spark-streaming-mqtt](https://spark.apache.org/docs/1.6.2/api/java/org/apache/spark/streaming/mqtt/package-summary.html)
package.

## Prerequisites

* Scala 2.10+
* sbt 0.13+ (if building from source)

## Dependencies

* [co.verdigris.security.specs.spec-pkcs1](https://github.com/VerdigrisTech/spec-pkcs1) package
* [org.eclipse.paho.client.mqttv3](https://github.com/eclipse/paho.mqtt.java) package

## Usage

### Submitting standalone Spark Streaming jobs
You can include this package into your standalone Spark Streaming package as a fat JAR or
provide it using the `--jars` option for Spark CLI tools (e.g. `spark-submit`).

If submitting the job with the `--jars` options, be sure to include the dependencies as well.

Thin JAR example:

```bash
$ spark-submit --jars spark-streaming-mqtts-0.1.0.jar,\
spec-pkcs1-0.1.0.jar,\
org.eclipse.paho.mqttv3.jar \
YOUR-STANDALONE-SPARK-APP.jar
```

## Building

Before you begin, you will need SBT version 0.13 or higher to build this project.

To manually build this package from source, just run:

```bash
$ sbt package
```

## License

Distributed under [Apache 2.0 license](http://www.apache.org/licenses/LICENSE-2.0).
