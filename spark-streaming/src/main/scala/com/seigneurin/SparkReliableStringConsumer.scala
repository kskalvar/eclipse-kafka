package com.seigneurin.strings

import com.ippontech.kafka.KafkaSource
import com.ippontech.kafka.stores.ZooKeeperOffsetsStore
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkReliableStringConsumer {

  def main(args: Array[String]) {
    val topic = "text-input"
    val conf = new SparkConf()
      .setAppName("eclipse-kafka")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(2))

    val offsetsStore = new ZooKeeperOffsetsStore("localhost:2181", s"/my-app/offsets/$topic")
    val dstream: InputDStream[(String, String)] = KafkaSource.kafkaStream
      [String, String, StringDecoder, StringDecoder](ssc, "localhost:9092", offsetsStore, topic)

    dstream.foreachRDD(messages => messages.foreach(processMessage))

    ssc.start()
    ssc.awaitTermination()
  }

  def processMessage(message: (String, String)) {
    val (key, value) = message
    if (value.contains("err")) {
      println("About to simulate an error")
      Thread.sleep(3000)
      throw new Exception("Simulating error")
    }
    println(value)
  }

}
