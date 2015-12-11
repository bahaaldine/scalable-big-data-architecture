package org.apress.examples.chapter4

import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import scala.util.parsing.json.JSON
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark
import scala.collection.mutable.HashMap
import breeze.linalg.Axis._0
import org.apache.spark.rdd.RDD
import scala.collection.mutable.MutableList

case class PageStatistic (
  verbs:List[Map[String, Integer]]
)

case class Clickstream (
   message:String,
   version:String,
   file:String,
   host:String,
   offset:String,
   eventType:String,
   clientip:String,
   ident:String,
   auth:String,
   timestamp:String,
   verb:String,
   request:String,
   httpVersion:String,
   response:String,
   bytes:Integer,
   referrer:String,
   agent:String
)

object KafkaStreamer {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(s"""
        |Usage: DirectKafkaWordCount <brokers> <topics>
        |  <brokers> is a list of one or more Kafka brokers
        |  <topics> is a list of one or more kafka topics to consume from
        |
        """.stripMargin)
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("KafkaStreamerToElasticsearch")
    sparkConf.set("es.index.auto.create", "true")
    sparkConf.set("es.nodes", "localhost:9200")
    //sparkConf.set("es.net.http.auth.user", "bahaaldine")
    //sparkConf.set("es.net.http.auth.pass", "bazarmi")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    
    // Create direct kafka stream with brokers and topics
    val Array(brokers, topics) = args
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_._2)
    val parsedEvents = lines.map(JSON.parseFull(_)).map(_.get.asInstanceOf[scala.collection.immutable.Map[String,Any]])
    val events = parsedEvents.map(data=>Clickstream(
       data("message").toString
       ,data("@version").toString
       ,data("file").toString
       ,data("host").toString
       ,data("offset").toString
       ,data("type").toString
       ,data("clientip").toString
       ,data("ident").toString
       ,data("auth").toString
       ,data("timestamp").toString
       ,data("verb").toString
       ,data("request").toString
       ,data("httpversion").toString
       ,data("response").toString
       ,Integer.parseInt(data("bytes").toString)
       ,data("referrer").toString
       ,data("agent").toString
    ))
    
    val counts = events.map(event => event.verb).countByValue()
    counts.print()
 
    counts.foreachRDD{ rdd =>
      if (rdd.toLocalIterator.nonEmpty) {
        var array:Array[(String, Long)] = rdd.collect()
        EsSpark.saveToEs(rdd, "spark/clickstream")
        //EsSpark.saveToEs(ssc.sparkContext.makeRDD(Seq(Map("id" -> 123, array(0)._1 -> array(0)._2))), "spark/clickstream", Map("es.mapping.id" -> "id"))    
      }
    }
    
    
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
