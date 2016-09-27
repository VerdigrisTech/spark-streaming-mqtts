/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package co.verdigris.spark.streaming.mqtts

import java.security.{KeyStore, PrivateKey}
import java.security.cert.X509Certificate
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.eclipse.paho.client.mqttv3._
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence

private[streaming]
class MQTTSInputDStream(
                       ssc : StreamingContext,
                       brokerUrl: String,
                       topic: String,
                       caCert: X509Certificate,
                       cert: X509Certificate,
                       privateKey: PrivateKey,
                       storageLevel: StorageLevel
                       ) extends ReceiverInputDStream[String](ssc) {
  def getReceiver(): Receiver[String] = {
    new MQTTSReceiver(brokerUrl, topic, caCert, cert, privateKey, storageLevel)
  }
}

private[streaming]
class MQTTSReceiver(
                   brokerUrl: String,
                   topic: String,
                   caCert: X509Certificate,
                   cert: X509Certificate,
                   privateKey: PrivateKey,
                   storageLevel: StorageLevel
                   ) extends Receiver[String](storageLevel) {
  def onStart(): Unit = {
    // Set up persistence for messages
    val persistence = new MemoryPersistence()

    // Initializing Mqtt Client specifying brokerUrl, clientID and MqttClientPersistance
    val client = new MqttClient(brokerUrl, MqttClient.generateClientId(), persistence)

    // Set connection options for the Mqtt Client
    val mqttConnectionOptions = new MqttConnectOptions()

    // Store CA certificate in JKS
    val caKeyStore = KeyStore.getInstance(KeyStore.getDefaultType)
    caKeyStore.load(null, null)
    caKeyStore.setCertificateEntry("ca-certificate", caCert)

    // Establish certificate trust chain
    val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    tmf.init(caKeyStore)

    // Deal with client key and certificates
    val clientKeyStore = KeyStore.getInstance(KeyStore.getDefaultType)
    clientKeyStore.load(null, null)
    clientKeyStore.setCertificateEntry("certificate", cert)
    clientKeyStore.setKeyEntry("private-key", privateKey, new Array[Char](0), Array(cert))

    val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    kmf.init(clientKeyStore, new Array[Char](0))

    // Set up client certificate for connection over TLS
    val sslContext = SSLContext.getInstance("TLSv1.2")
    sslContext.init(kmf.getKeyManagers, tmf.getTrustManagers, null)

    // Tell MQTT Options that we're using socket over TLS
    mqttConnectionOptions.setSocketFactory(sslContext.getSocketFactory)

    // Callback automatically triggers as and when new message arrives on specified topic
    val callback = new MqttCallback() {

      // Handles Mqtt message
      override def messageArrived(topic: String, message: MqttMessage) {
        store(new String(message.getPayload, "utf-8"))
      }

      override def deliveryComplete(token: IMqttDeliveryToken) {
      }

      override def connectionLost(cause: Throwable) {
        restart("Connection lost ", cause)
      }
    }

    // Set up callback for MqttClient. This needs to happen before
    // connecting or subscribing, otherwise messages may be lost
    client.setCallback(callback)

    // Connect to MqttBroker
    client.connect(mqttConnectionOptions)

    // Subscribe to Mqtt topic
    client.subscribe(topic)

    if (client.isConnected()) {
      println(s"MQTT client $client.getClientId is connected")
    } else {
      println(s"MQTT client $client.getClientId is NOT connected")
    }
  }

  def onStop(): Unit = {}
}