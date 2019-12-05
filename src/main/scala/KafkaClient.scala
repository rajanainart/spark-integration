package com.rajanainart

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import scala.collection.mutable
import scala.runtime.BoxedUnit

class KafkaClient extends java.io.Closeable{

  val KAFKA_SERVER_CONFIG_KEY = "bootstrap.servers"
  val KAFKA_GROUP_CONFIG_KEY  = "group.id"
  val KAFKA_OFFSET_CONFIG_KEY = "auto.offset.reset"
  val KAFKA_COMMIT_CONFIG_KEY = "enable.auto.commit"
  val KAFKA_KEY_DESERIALIZER_KEY = "key.deserializer"
  val KAFKA_VAL_DESERIALIZER_KEY = "value.deserializer"

  val parameters = mutable.Map[String, Object](
    KAFKA_SERVER_CONFIG_KEY -> Common.kafkaConfig.getJsonProperty(KAFKA_SERVER_CONFIG_KEY).toString,
    KAFKA_GROUP_CONFIG_KEY  -> Common.kafkaConfig.getJsonProperty(KAFKA_GROUP_CONFIG_KEY ).toString,
    KAFKA_OFFSET_CONFIG_KEY -> (if (Common.kafkaConfig.getJsonProperty(KAFKA_OFFSET_CONFIG_KEY) == null) "latest"
                                else Common.kafkaConfig.getJsonProperty(KAFKA_OFFSET_CONFIG_KEY).toString),
    KAFKA_COMMIT_CONFIG_KEY -> (if (Common.kafkaConfig.getJsonProperty(KAFKA_COMMIT_CONFIG_KEY) == null) java.lang.Boolean.valueOf(false)
                                else java.lang.Boolean.valueOf(Common.kafkaConfig.getJsonProperty(KAFKA_COMMIT_CONFIG_KEY).toString)),
    KAFKA_KEY_DESERIALIZER_KEY -> classOf[StringDeserializer],
    KAFKA_VAL_DESERIALIZER_KEY -> classOf[StringDeserializer]
  )

  val customProperties = Common.kafkaConfig.getJsonPropertiesStartsWith("property.")
  for (key <- customProperties.keySet)
    parameters.put(key.replaceAll("property.", ""), customProperties.get(key).get.toString)

  private val mns : Any = (if  (Common.kafkaConfig.getJsonProperty("poll.duration.mns") == BoxedUnit.UNIT) 1
                           else Common.kafkaConfig.getJsonProperty("poll.duration.mns"))

  val pollDuration     = mns.asInstanceOf[Double]*60
  var streamingContext : StreamingContext = null;

  def consume(): Unit = {
    if (streamingContext == null)
      streamingContext = new StreamingContext(Common.sparkSession.sparkContext, Seconds(pollDuration.asInstanceOf[Long]))

    /*if (!Common.kafkaConfig.getJsonProperty("jaas.config.path").toString.isEmpty)
      System.setProperty("java.security.auth.login.config", Common.kafkaConfig.getJsonProperty("jaas.config.path").toString)
    if (!Common.kafkaConfig.getJsonProperty("kerberos-config-path").toString.isEmpty)
      System.setProperty("java.security.krb5.conf", Common.kafkaConfig.getJsonProperty("kerberos-config-path").toString)*/

    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](Seq(Common.kafkaConfig.getJsonProperty("topic.name").toString), parameters)
    )

    val map = stream.map(record => (record.key(), (record.topic(), record.partition(), record.offset(), record.value())))
    map.foreachRDD(x => {
      if (x.count() > 0) {
        for (key <- x.keys.take(x.count().toInt)) {
          if (key.startsWith("integration-meta-for-spark")) {
            val seq = x.lookup(key)(0)
            val topic     = seq._1
            val partition = seq._2
            val offset    = seq._3
            val meta      = seq._4
            println(printf("Topic:%s, Partition:%d, Offset:%d, Key:%s", topic, partition, offset, key))
            println(meta)
            val imp : Import = new Import(meta)
            try {
              imp.start()
            }
            catch {
              case x : Exception => {
                x.printStackTrace()
                if (imp != null)
                  Common.log(imp.getProcessId(), String.format("Exception while loading records to the target:%s", x.getLocalizedMessage()))
              }
            }
          }
        }
      }
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  override def close(): Unit = {
  }
}
