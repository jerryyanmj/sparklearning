package streaming

import kafka.serializer.StringDecoder
import model.{GeminiParsed, GeminiRawDecoder, GeminiRaw}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by jiarui.yan on 5/29/15.
 */
object KafkaApp1 {

  def main(args: Array[String]) {

    val Array(appName, brokerList, topic, rddInterval) = args

    // sbt "run-main streaming.KafkaApp1 local[2] Kafka-Integration dnvrco01-os-coe0002.conops.timewarnercable.com:9092 gemini_v01 5"


    //val conf = new SparkConf().setMaster("local[2]").setAppName("Kafka Integration Test")
    val conf = new SparkConf().setAppName(appName)
    val ssc = new StreamingContext(conf, Seconds(rddInterval.toLong))

    // Connecting to Kafka queue for Gemini 71.74.186.212:9092
    // "dnvrco01-os-coe0002.conops.timewarnercable.com:9092"
    val kafkaParams = Map("metadata.broker.list" -> brokerList)

    // "gemini_v03"
    val topics = Set(topic)

    val events = KafkaUtils.createDirectStream[String, GeminiRaw, StringDecoder, GeminiRawDecoder](ssc, kafkaParams, topics).map(_._2)

    val parsedEvents = events map (gr => new GeminiParsed(gr))

    val logLineByEdgeNodesParsed = parsedEvents.map(l => (l.sDns.get, 1)).reduceByKey((x,y) => x + y)
    logLineByEdgeNodesParsed.print(50)

    val eventTypeCount = parsedEvents map (l => (l.eventType.get, 1)) reduceByKey((x,y) => x+y)
    eventTypeCount.print(50)

    val cacheStatusCount = parsedEvents map (l => (l.cacheDeliveryType.get, 1)) reduceByKey((x,y) => x+y)
    cacheStatusCount.print(50)

    val httpStatusCount = parsedEvents map (l => (l.httpStatusCode.get, 1)) reduceByKey((x,y) => x+y)
    httpStatusCount.print(50)

    val contentSize = parsedEvents map (l => (l.cacheDeliveryType.get, l.contentSize.getOrElse(0L))) reduceByKey((x,y) => x+y)
    contentSize.print(50)

//    val splittedRdd = events.map(rdd => rdd.split('\t'))
//
//    val fullPartialLogCount = splittedRdd.map(lArr => (lArr.length, 1)).reduceByKey((x, y) => x + y)
//
//    fullPartialLogCount.print(50)
//
//    val partialLogSample = splittedRdd.filter(lArr => lArr.length < 15).map(lArr => lArr.mkString("|"))
//
//    partialLogSample.print(5)
//
//    val fullLogSample = splittedRdd.filter(lArr => lArr.length > 15).map(lArr => lArr.mkString("|"))
//
//    fullLogSample.print(5)



    ssc.start()
    ssc.awaitTermination()


  }

}
