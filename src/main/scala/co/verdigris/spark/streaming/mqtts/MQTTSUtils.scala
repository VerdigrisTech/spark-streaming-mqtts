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

import java.io.ByteArrayInputStream
import java.security.{KeyFactory, PrivateKey}
import java.security.cert.{CertificateFactory, X509Certificate}

import co.verdigris.security.spec.PKCS1EncodedKeySpec

import scala.reflect.ClassTag
import org.apache.commons.codec.binary.Base64
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaReceiverInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import sun.security.provider.X509Factory

object MQTTSUtils {
  /**
    * Create an input stream that receives messages pushed by a MQTT publisher.
    *
    * @param jssc       JavaStreamingContext object
    * @param brokerUrl  Url of remote MQTT publisher
    * @param topic      Topic name to subscribe to
    * @param caCert     Certificate of Certificate Authority of remote MQTT publisher
    * @param cert       Client certificate associated with the MQTT client
    * @param privateKey :  Private key associated with the MQTT client
    */
  def createBinaryStream(
                    jssc: JavaStreamingContext,
                    brokerUrl: String,
                    topic: String,
                    caCert: X509Certificate,
                    cert: X509Certificate,
                    privateKey: PrivateKey
                  ): JavaReceiverInputDStream[Array[Byte]] = {
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[Array[Byte]]]
    createBinaryStream(jssc.ssc, brokerUrl, topic, caCert, cert, privateKey)
  }

  /**
    * Create an input stream that receives messages pushed by a MQTT publisher.
    *
    * @param jssc         JavaStreamingContext object
    * @param brokerUrl    Url of remote MQTT publisher
    * @param topic        Topic name to subscribe to
    * @param caCert       Certificate of Certificate Authority of remote MQTT publisher
    * @param cert         Client certificate associated with the MQTT client
    * @param privateKey   Private key associated with the MQTT client
    * @param storageLevel RDD storage level.
    */
  def createBinaryStream(
                    jssc: JavaStreamingContext,
                    brokerUrl: String,
                    topic: String,
                    caCert: X509Certificate,
                    cert: X509Certificate,
                    privateKey: PrivateKey,
                    storageLevel: StorageLevel
                  ): JavaReceiverInputDStream[Array[Byte]] = {
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[Array[Byte]]]
    createBinaryStream(jssc.ssc, brokerUrl, topic, caCert, cert, privateKey, storageLevel)
  }

  /**
    * Create an input stream that receives messages pushed by a MQTT publisher.
    *
    * @param ssc          StreamingContext object
    * @param brokerUrl    Url of remote MQTT publisher
    * @param topic        Topic name to subscribe to
    * @param caCert       Certificate Authority's certificate in base64 encoded PEM formatted string
    * @param cert         Client certificate in base64 encoded PEM formatted string
    * @param privateKey   Private key in PKCS8 formatted string
    */
  def createBinaryStream(
                    ssc: StreamingContext,
                    brokerUrl: String,
                    topic: String,
                    caCert: String,
                    cert: String,
                    privateKey: String
                  ): ReceiverInputDStream[Array[Byte]] = {
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[Array[Byte]]]
    val _caCert = stringToX509Certificate(caCert)
    val _cert = stringToX509Certificate(cert)
    val _privateKey = stringToPrivateKey(privateKey)

    createBinaryStream(ssc, brokerUrl, topic, _caCert, _cert, _privateKey)
  }

  /**
    * Create an input stream that receives messages pushed by a MQTT publisher.
    *
    * @param ssc          StreamingContext object
    * @param brokerUrl    Url of remote MQTT publisher
    * @param topic        Topic name to subscribe to
    * @param caCert       Certificate Authority's certificate in base64 encoded PEM formatted string
    * @param cert         Client certificate in base64 encoded PEM formatted string
    * @param privateKey   Private key in PKCS8 formatted string
    * @param storageLevel RDD storage level.
    */
  def createBinaryStream(
                    ssc: StreamingContext,
                    brokerUrl: String,
                    topic: String,
                    caCert: String,
                    cert: String,
                    privateKey: String,
                    storageLevel: StorageLevel
                  ): ReceiverInputDStream[Array[Byte]] = {
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[Array[Byte]]]
    val _caCert = stringToX509Certificate(caCert)
    val _cert = stringToX509Certificate(cert)
    val _privateKey = stringToPrivateKey(privateKey)

    createBinaryStream(ssc, brokerUrl, topic, _caCert, _cert, _privateKey, storageLevel)
  }

  /**
    * Create an input stream that receives messages pushed by a MQTT publisher.
    *
    * @param ssc          StreamingContext object
    * @param brokerUrl    Url of remote MQTT publisher
    * @param topic        Topic name to subscribe to
    * @param caCert       Certificate of Certificate Authority of remote MQTT publisher
    * @param cert         Client certificate associated with the MQTT client
    * @param privateKey   Private key associated with the MQTT client
    * @param storageLevel RDD storage level.
    */
  def createBinaryStream(
                    ssc: StreamingContext,
                    brokerUrl: String,
                    topic: String,
                    caCert: X509Certificate,
                    cert: X509Certificate,
                    privateKey: PrivateKey,
                    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
                  ): ReceiverInputDStream[Array[Byte]] = {
    new MQTTSBinaryInputDStream(ssc, brokerUrl, topic, caCert, cert, privateKey, storageLevel)
  }

  /**
    * Create an input stream that receives messages pushed by a MQTT publisher.
    *
    * @param jssc       JavaStreamingContext object
    * @param brokerUrl  Url of remote MQTT publisher
    * @param topic      Topic name to subscribe to
    * @param caCert     Certificate of Certificate Authority of remote MQTT publisher
    * @param cert       Client certificate associated with the MQTT client
    * @param privateKey :  Private key associated with the MQTT client
    */
  def createStream(
                    jssc: JavaStreamingContext,
                    brokerUrl: String,
                    topic: String,
                    caCert: X509Certificate,
                    cert: X509Certificate,
                    privateKey: PrivateKey
                  ): JavaReceiverInputDStream[String] = {
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createStream(jssc.ssc, brokerUrl, topic, caCert, cert, privateKey)
  }

  /**
    * Create an input stream that receives messages pushed by a MQTT publisher.
    *
    * @param jssc         JavaStreamingContext object
    * @param brokerUrl    Url of remote MQTT publisher
    * @param topic        Topic name to subscribe to
    * @param caCert       Certificate of Certificate Authority of remote MQTT publisher
    * @param cert         Client certificate associated with the MQTT client
    * @param privateKey   Private key associated with the MQTT client
    * @param storageLevel RDD storage level.
    */
  def createStream(
                    jssc: JavaStreamingContext,
                    brokerUrl: String,
                    topic: String,
                    caCert: X509Certificate,
                    cert: X509Certificate,
                    privateKey: PrivateKey,
                    storageLevel: StorageLevel
                  ): JavaReceiverInputDStream[String] = {
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createStream(jssc.ssc, brokerUrl, topic, caCert, cert, privateKey, storageLevel)
  }

  /**
    * Create an input stream that receives messages pushed by a MQTT publisher.
    *
    * @param ssc          StreamingContext object
    * @param brokerUrl    Url of remote MQTT publisher
    * @param topic        Topic name to subscribe to
    * @param caCert       Certificate Authority's certificate in base64 encoded PEM formatted string
    * @param cert         Client certificate in base64 encoded PEM formatted string
    * @param privateKey   Private key in PKCS8 formatted string
    */
  def createStream(
                    ssc: StreamingContext,
                    brokerUrl: String,
                    topic: String,
                    caCert: String,
                    cert: String,
                    privateKey: String
                  ): ReceiverInputDStream[String] = {
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    val _caCert = stringToX509Certificate(caCert)
    val _cert = stringToX509Certificate(cert)
    val _privateKey = stringToPrivateKey(privateKey)

    createStream(ssc, brokerUrl, topic, _caCert, _cert, _privateKey)
  }

  /**
    * Create an input stream that receives messages pushed by a MQTT publisher.
    *
    * @param ssc          StreamingContext object
    * @param brokerUrl    Url of remote MQTT publisher
    * @param topic        Topic name to subscribe to
    * @param caCert       Certificate Authority's certificate in base64 encoded PEM formatted string
    * @param cert         Client certificate in base64 encoded PEM formatted string
    * @param privateKey   Private key in PKCS8 formatted string
    * @param storageLevel RDD storage level.
    */
  def createStream(
                    ssc: StreamingContext,
                    brokerUrl: String,
                    topic: String,
                    caCert: String,
                    cert: String,
                    privateKey: String,
                    storageLevel: StorageLevel
                  ): ReceiverInputDStream[String] = {
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    val _caCert = stringToX509Certificate(caCert)
    val _cert = stringToX509Certificate(cert)
    val _privateKey = stringToPrivateKey(privateKey)

    createStream(ssc, brokerUrl, topic, _caCert, _cert, _privateKey, storageLevel)
  }

  /**
    * Create an input stream that receives messages pushed by a MQTT publisher.
    *
    * @param ssc          StreamingContext object
    * @param brokerUrl    Url of remote MQTT publisher
    * @param topic        Topic name to subscribe to
    * @param caCert       Certificate of Certificate Authority of remote MQTT publisher
    * @param cert         Client certificate associated with the MQTT client
    * @param privateKey   Private key associated with the MQTT client
    * @param storageLevel RDD storage level.
    */
  def createStream(
                    ssc: StreamingContext,
                    brokerUrl: String,
                    topic: String,
                    caCert: X509Certificate,
                    cert: X509Certificate,
                    privateKey: PrivateKey,
                    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
                  ): ReceiverInputDStream[String] = {
    new MQTTSInputDStream(ssc, brokerUrl, topic, caCert, cert, privateKey, storageLevel)
  }

  private def stringToX509Certificate(cert: String): X509Certificate = {
    val cf = CertificateFactory.getInstance("X.509")

    cf.generateCertificate(
      new ByteArrayInputStream(
        Base64.decodeBase64(cert.stripPrefix(X509Factory.BEGIN_CERT).stripSuffix(X509Factory.END_CERT))
      )
    ).asInstanceOf[X509Certificate]
  }

  private def stringToPrivateKey(privateKey: String, algorithm: String = "RSA"): PrivateKey = {
    val kf = KeyFactory.getInstance(algorithm)
    val keySpec = new PKCS1EncodedKeySpec(
      Base64.decodeBase64(
        privateKey.stripPrefix("-----BEGIN RSA PRIVATE KEY-----")
                  .stripSuffix("-----END RSA PRIVATE KEY-----")
      )
    )

    kf.generatePrivate(keySpec)
  }
}