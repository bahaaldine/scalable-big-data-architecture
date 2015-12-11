package org.apache.spark.examples

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
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.regression.LabeledPoint

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

case class Customer (
   session:String,
   request:String,
   category:String
)


object SparkEnricher {
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

    val productCategoryMappingFile = ssc.sparkContext.textFile("/Users/bahaaldine/Google Drive/demo/v2/clickstream/generator/mappings.csv")
    val productCategoryMapping = productCategoryMappingFile.map(line => line.split(",")).map(x => (x(0),x(1))).collectAsMap()
    val categoryLabelMapping:scala.collection.Map[String,Double] = productCategoryMappingFile.map(line => line.split(",")).map(x => (x(1),x(2).toDouble)).collectAsMap()
    val brodcastProductCategoryMapping = ssc.sparkContext.broadcast(productCategoryMapping)
    val brodcastCategoryLabelMapping = ssc.sparkContext.broadcast(categoryLabelMapping)
    
    val customerMappingFile = ssc.sparkContext.textFile("/Users/bahaaldine/Google Drive/demo/v2/clickstream/generator/ip_mappings.csv")
    val ipLabelMapping:scala.collection.Map[String,Double] = customerMappingFile.map(line => line.split(",")).map(x => (x(0),x(1).toDouble)).collectAsMap()
    val brodcastIpLabelMapping = ssc.sparkContext.broadcast(ipLabelMapping)
    
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
    
    // Creating and enriching the customer object
    val customers = events.map { clickstream => 
       val lookupMap = brodcastProductCategoryMapping.value
       Customer(clickstream.clientip, clickstream.request, lookupMap.getOrElse(clickstream.request, "category not found"))
    }

    customers.foreachRDD{ rdd =>
      if (rdd.toLocalIterator.nonEmpty) {
        EsSpark.saveToEs(rdd, "spark/customer")    
      }
    }
    
    val trainingData = customers.map { customer =>
      val categoryLookupMap = brodcastCategoryLabelMapping.value
      val customerLookupMap = brodcastIpLabelMapping.value
      
      val categoryLabel = categoryLookupMap.getOrElse(customer.category, 1).asInstanceOf[Double]
      val customerLabel = customerLookupMap.getOrElse(customer.session, 1).asInstanceOf[Double]
      
      Vectors.dense(Array(categoryLabel, customerLabel))
    }
    
    val testData = customers.map { customer =>
      val categoryLookupMap = brodcastCategoryLabelMapping.value
      val customerLookupMap = brodcastIpLabelMapping.value
      
      val categoryLabel = categoryLookupMap.getOrElse(customer.category, 1).asInstanceOf[Double]
      val customerLabel = customerLookupMap.getOrElse(customer.session, 1).asInstanceOf[Double]
      
      LabeledPoint(categoryLabel, Vectors.dense(Array(categoryLabel, customerLabel)))
    }
    
    val model = new StreamingKMeans()
      .setK(3)
      .setDecayFactor(1.0)
      .setRandomCenters(2, 0.0)

    model.trainOn(trainingData)
    model.predictOnValues(testData.map(lp => (lp.label, lp.features))).foreachRDD{ rdd =>
      if (rdd.toLocalIterator.nonEmpty) {
        EsSpark.saveToEs(rdd, "spark/prediction")    
      }
    }

        
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
