package streaming

import model._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf

/**
 * Created by jiarui.yan on 5/29/15.
 *
 * ~/Works/tools/spark-1.1.0-bin-mapr3/bin/spark-submit \
--class "streaming.KafkaApp1" \
--master local[2] \
target/scala-2.10/sparklearning-assembly-1.0.jar \
KafkaIntegrationTest \
dnvrco01-os-coe0002.conops.timewarnercable.com:2181 \
gemini-consumer \
gemini_v01 \
2 \
10
 *
 */

trait helper {
  def sumInt(f: Int => Int): (Int, Int) => Int = (x: Int, y: Int) => f(x) + f(y)
  def sumLong(f: Long => Long): (Long, Long) => Long = (x: Long, y: Long) => f(x) + f(y)
}

object KafkaApp1 extends helper {

  def main(args: Array[String]) {

    val Array(appName, zkQuorum, group, topics, numThreads, rddInterval) = args

    val conf = new SparkConf().setAppName(appName)
    val ssc = new StreamingContext(conf, Seconds(rddInterval.toLong))
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val strEventStream = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

//    strEventStream map (l => {
//      if (l.indexOf("linear") != -1) ("Linear", 1)
//      else if (l.indexOf("video") != -1) ("VOD", 1)
//      else if (l.indexOf("vod") != -1) ("VOD-2", 1)
//      else ("Other", 1)
//    }) reduceByKey((x, y) => x + y) print()
//
//    strEventStream filter (l => l.indexOf("video") != -1) print()
//    strEventStream filter (l => l.indexOf("vod") != -1) print()

    val splittedRdd = strEventStream.map(rdd => rdd.split('\t'))

    splittedRdd map(lArr => (lArr.length, 1)) reduceByKey((x, y) => x + y) print()
    //splittedRdd.filter(lArr => lArr.length < 15).map(lArr => lArr.mkString("|")) print(5)
    //splittedRdd.filter(lArr => lArr.length > 15).map(lArr => lArr.mkString("|")) print(5)

    //val decodedEvents = KafkaUtils.createDirectStream[String, GeminiRaw, StringDecoder, GeminiRawDecoder](ssc, kafkaParams, topics).map(_._2)

    val regularEvent = strEventStream map (l => l.split('\t')) filter (lArr => lArr.length == 18)
    val regularRawEvent = regularEvent map (lArr => new GeminiRaw(lArr))
    val regularParsedEvents = regularRawEvent map (gr => new GeminiParsed(gr))

    val logLineCountByEdge = regularParsedEvents.map(l => (l.sDns.get, 1)) reduceByKeyAndWindow (sumInt(x => x), Seconds(rddInterval.toInt), Seconds(rddInterval.toInt))
    logLineCountByEdge.foreachRDD((rdd: RDD[(String, Int)], time: Time) => {
      println("\n-------------------")
      println("Time: " + time)
      println("-------------------")
      println("Received " + rdd.count() + " events\n")
      rdd.foreach(l => {
        println("Edge node name " + l._1 + " received  " + l._2 + " events.")
      })
      val total = rdd.reduce((p1, p2) => ("Total", p1._2 + p2._2))
      println("Total count " + total + " events.")
    })

    regularParsedEvents map (l => (l.chunkType.getOrElse("Other"), 1)) reduceByKeyAndWindow (sumInt(x => x), Seconds(30), Seconds(10)) print()
    regularParsedEvents map (l => (l.streamType.getOrElse("Other"), 1)) reduceByKeyAndWindow (sumInt(x => x), Seconds(30), Seconds(10)) print()
    regularParsedEvents map (l => (l.eventType.get, 1)) reduceByKeyAndWindow (sumInt(x => x), Seconds(30), Seconds(10)) print()
    regularParsedEvents map (l => (l.httpStatusCode.get, 1)) reduceByKeyAndWindow (sumInt(x => x), Seconds(30), Seconds(10)) print()
    regularParsedEvents map (l => (l.cacheDeliveryType.getOrElse("Unknown"), 1)) reduceByKeyAndWindow (sumInt(x => x), Seconds(30), Seconds(10)) print()
    regularParsedEvents map (l => (l.cacheDeliveryType.getOrElse("Unknown"), l.contentSize.getOrElse(0L))) reduceByKeyAndWindow (sumLong(x => x), Seconds(30), Seconds(10)) print()
    regularParsedEvents map (l => (l.mappedUserAgent.get, 1)) reduceByKeyAndWindow (sumInt(x => x), Seconds(30), Seconds(10)) print()

//    val midTierEvent = strEventStream map (l => l.split('\t')) filter (lArr => lArr.length == 10)
//    val midTierRawEvent = midTierEvent map (lArr => new GeminiRawMiddleTier(lArr))
//    val midTierParsedEvents = midTierRawEvent map (l => new GeminiParsedMiddleTier(l))
//
//    midTierParsedEvents.filter(l => l.csMethod.contains("CNN") && l.csMethod.contains(".ts")) map (l => (l.csMethod, 1)) reduceByKeyAndWindow (sumInt(x => x), Seconds(30), Seconds(10)) print()
//
//    regularRawEvent map (re => {
//
//      val userAgent = re.cs_user_agent
//      if (userAgent.contains("http_cli/0.4")) ("http_cli/0.4", 1)
//      else (userAgent, 1)
//
//    }) reduceByKeyAndWindow (sumInt(x => x), Seconds(30), Seconds(10)) foreachRDD((rdd: RDD[(String, Int)], time: Time) => {
//      println("\n-------------------")
//      println("Time: " + time)
//      println("-------------------")
//      rdd.foreach(l => {
//        println("User Agent " + l._1 + " received  " + l._2 + " events.")
//      })
//      val total = rdd.reduce((p1, p2) => ("Total", p1._2 + p2._2))
//      println("Total count " + total + " events.")
//    })
//
//    midTierRawEvent map (re => {
//
//      val userAgent = re.cs_user_agent
//      if (userAgent.contains("http_cli/0.4")) ("http_cli/0.4", 1)
//      else (userAgent, 1)
//
//    }) reduceByKeyAndWindow (sumInt(x => x), Seconds(30), Seconds(10)) foreachRDD((rdd: RDD[(String, Int)], time: Time) => {
//      println("\n-------------------")
//      println("Time: " + time)
//      println("-------------------")
//      rdd.foreach(l => {
//        println("User Agent " + l._1 + " received  " + l._2 + " events.")
//      })
//      val total = rdd.reduce((p1, p2) => ("Total", p1._2 + p2._2))
//      println("Total count " + total + " events.")
//    })


    ssc.start()
    ssc.awaitTermination()
  }

}
