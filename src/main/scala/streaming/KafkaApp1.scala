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

    val logLineByEdgeNodesParsed = parsedEvents.map(l => (l.sDns, 1)).reduceByKey((x,y) => x + y)
    logLineByEdgeNodesParsed.print(50)

    val eventTypeCount = parsedEvents map (l => (l.eventType map _, 1)) reduceByKey((x,y) => x+y)
    eventTypeCount.print(50)


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


//
//
//    // Total content size of HIT and MISS
//    events.filter(l => l.split('\t').length >= 17).map(l => {
//      val fields = l.split('\t')
//      val hit = fields(7).toLong
//      val missed = if ("MISS".equals(fields(12))) hit else 0L
//      (hit, missed)
//    }).reduce( {case ((x,y),(a,b)) => (x+y, a+b)} ).print()

    ssc.start()
    ssc.awaitTermination()


  }

}
