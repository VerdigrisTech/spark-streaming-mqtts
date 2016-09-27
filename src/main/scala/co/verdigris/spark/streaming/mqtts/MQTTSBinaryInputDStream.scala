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

import java.security.PrivateKey
import java.security.cert.X509Certificate

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver

private[streaming]
class MQTTSBinaryInputDStream(
                         ssc : StreamingContext,
                         brokerUrl: String,
                         topic: String,
                         caCert: X509Certificate,
                         cert: X509Certificate,
                         privateKey: PrivateKey,
                         storageLevel: StorageLevel
                       ) extends ReceiverInputDStream[Array[Byte]](ssc) {
  def getReceiver(): Receiver[Array[Byte]] = {
    new MQTTSBinaryReceiver(brokerUrl, topic, caCert, cert, privateKey, storageLevel)
  }
}

private[streaming]
class MQTTSBinaryReceiver(
                     brokerUrl: String,
                     topic: String,
                     caCert: X509Certificate,
                     cert: X509Certificate,
                     privateKey: PrivateKey,
                     storageLevel: StorageLevel
                   ) extends MQTTSReceiver[Array[Byte]](brokerUrl, topic, caCert, cert, privateKey, storageLevel) {
  def processMessage(payload: Array[Byte]): Unit = {
    store(payload)
  }
}
