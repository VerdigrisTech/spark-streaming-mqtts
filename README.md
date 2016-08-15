# spark-streaming-mqtts

This module adds client side SSL certificate and TLS v1.2 support to the
`MQTTUtils` class provided by [spark-streaming-mqtt](https://spark.apache.org/docs/1.6.2/api/java/org/apache/spark/streaming/mqtt/package-summary.html)
package.

## Prerequisites

* Scala 2.10+
* sbt 0.13+

## Dependencies

* [co.verdigris.security.specs.spec-pkcs1](https://github.com/VerdigrisTech/spec-pkcs1) package
* [org.eclipse.paho.client.mqttv3](https://github.com/eclipse/paho.mqtt.java) package

## Usage

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
