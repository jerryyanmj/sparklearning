package streaming

import kafka.serializer.StringDecoder
import model._
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

    // "gemini_v01"
    val topics = Set(topic)

    val strEventStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics).map(_._2)

    val splittedRdd = strEventStream.map(rdd => rdd.split('\t'))

    splittedRdd.map(lArr => (lArr.length, 1)).reduceByKey((x, y) => x + y) print(50)
    //splittedRdd.filter(lArr => lArr.length < 15).map(lArr => lArr.mkString("|")) print(5)
    //splittedRdd.filter(lArr => lArr.length > 15).map(lArr => lArr.mkString("|")) print(5)

    //val decodedEvents = KafkaUtils.createDirectStream[String, GeminiRaw, StringDecoder, GeminiRawDecoder](ssc, kafkaParams, topics).map(_._2)

    val regularEvent = strEventStream map (l => l.split('\t')) filter (lArr => lArr.length == 18)
    val parsedEvents = regularEvent map (lArr => new GeminiRaw(lArr)) map (gr => new GeminiParsed(gr))

    parsedEvents.map(l => (l.sDns.get, 1)) reduceByKeyAndWindow ((x:Int,y:Int) => (x + y), Seconds(30), Seconds(10)) print(50)
    parsedEvents map (l => (l.eventType.get, 1)) reduceByKeyAndWindow ((x:Int,y:Int) => (x + y), Seconds(30), Seconds(10)) print(50)
    parsedEvents map (l => (l.cacheDeliveryType.getOrElse("Unknown"), 1)) reduceByKeyAndWindow ((x:Int,y:Int) => (x + y), Seconds(30), Seconds(10)) print(50)
    parsedEvents map (l => (l.httpStatusCode.get, 1)) reduceByKeyAndWindow ((x:Int,y:Int) => (x + y), Seconds(30), Seconds(10)) print(50)
    parsedEvents map (l => (l.cacheDeliveryType.getOrElse("Unknown"), l.contentSize.getOrElse(0L))) reduceByKeyAndWindow ((x:Long,y:Long) => (x + y), Seconds(30), Seconds(10)) print(50)

    val midTierEvent = strEventStream map (l => l.split('\t')) filter (lArr => lArr.length == 10)
    val parsedMidTierEvents = midTierEvent map (lArr => new GeminiRawMiddleTier(lArr)) map (l => new GeminiParsedMiddleTier(l))

    parsedMidTierEvents.filter(l => l.csMethod.contains("CNN") && l.csMethod.contains(".ts")) map (l => (l.csMethod, 1)) reduceByKeyAndWindow ((x:Int,y:Int) => (x + y), Seconds(30), Seconds(10)) print(50)

    ssc.start()
    ssc.awaitTermination()


  }

}
