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
import org.apache.spark.streaming.receiver.Receiver
import org.eclipse.paho.client.mqttv3._
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence

private[streaming]
abstract class MQTTSReceiver[T](
                                                 brokerUrl: String,
                                                 topic: String,
                                                 caCert: X509Certificate,
                                                 cert: X509Certificate,
                                                 privateKey: PrivateKey,
                                                 storageLevel: StorageLevel
                                               ) extends Receiver[T](storageLevel) {
  protected val client = new MqttClient(brokerUrl, MqttClient.generateClientId, new MemoryPersistence())
  protected val connectOptions = new MqttConnectOptions()
  protected val callback = new MqttCallback() {
    override def messageArrived(topic: String, message: MqttMessage): Unit = {
      processMessage(message.getPayload)
    }

    override def deliveryComplete(iMqttDeliveryToken: IMqttDeliveryToken) {}

    override def connectionLost(cause: Throwable): Unit = {
      restart("Connection lost ", cause)
    }
  }
  protected val caKeyStore = KeyStore.getInstance(KeyStore.getDefaultType)
  protected val clientKeyStore = KeyStore.getInstance(KeyStore.getDefaultType)
  protected val keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
  protected val trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
  protected val sslContext = SSLContext.getInstance("TLSv1.2")


  def processMessage(payload: Array[Byte])

  def onStart(): Unit = {
    // Store CA certificate in JKS
    caKeyStore.load(null, null)
    caKeyStore.setCertificateEntry("ca-certificate", caCert)

    // Establish certificate trust chain
    trustManagerFactory.init(caKeyStore)

    // Store client certificate and private key in JKS
    clientKeyStore.load(null, null)
    clientKeyStore.setCertificateEntry("certificate", cert)
    clientKeyStore.setKeyEntry("private-key", privateKey, new Array[Char](0), Array(cert))

    // Initialize key manager from the key store
    keyManagerFactory.init(clientKeyStore, new Array[Char](0))

    // Set up client certificate for connection over TLS 1.2
    sslContext.init(keyManagerFactory.getKeyManagers, trustManagerFactory.getTrustManagers, null)

    // Set connect options to use the TLS enabled socket factory
    connectOptions.setSocketFactory(sslContext.getSocketFactory)

    // Set callback for MqttClient. This needs to happen before connecting or subscribing, other message may be lost.
    client.setCallback(callback)

    // Connect to MqttBroker
    client.connect(connectOptions)

    // Subscribe to MQTT topic
    client.subscribe(topic)
  }

  def onStop(): Unit = {}
}
