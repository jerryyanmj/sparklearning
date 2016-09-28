package streaming

import java.util

import model._
import model.cdn.{GeminiParsed, GeminiRaw}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf

import scala.util.matching.Regex

/**
 * Created by jiarui.yan on 5/29/15.
 *
 * /Users/jiarui.yan/tools/spark-1.2.2-bin-cdh4/bin/spark-submit \
*--class "streaming.KafkaApp1" \
*--master local[2] \
*target/scala-2.10/sparklearning-assembly-1.0.jar \
*KafkaIntegrationTest \
*opstkcld-vm-kfa0001.conops.timewarnercable.com:2181,opstkcld-vm-kfa0002.conops.timewarnercable.com:2181,opstkcld-vm-kfa0003.conops.timewarnercable.com:2181 \
*gemini-consumer \
*gemini_v02 \
*2 \
*10 \
*60 \
*20
 *
 */

trait helper {
  def sumInt(f: Int => Int): (Int, Int) => Int = (x: Int, y: Int) => f(x) + f(y)
  def sumLong(f: Long => Long): (Long, Long) => Long = (x: Long, y: Long) => f(x) + f(y)
  def sumStats: ((Long,Long,Long,Long,Long),(Long,Long,Long,Long,Long)) => (Long,Long,Long,Long,Long)
  = (stats1:(Long,Long,Long,Long,Long), stats2:(Long,Long,Long,Long,Long)) => (stats1._1+stats2._1,stats1._2+stats2._2,stats1._3+stats2._3,(if(stats1._4>stats2._4) stats1._4 else stats2._4), (if(stats1._5<stats2._5) stats1._5 else stats2._5))
}

trait bowserDetector {
  val MOZZILA = new Regex("")
  def isFirefox(str: String) = MOZZILA findAllIn str
}

object KafkaApp1 extends helper {

  def main(args: Array[String]) {

    val Array(appName, zkQuorum, group, topics, numThreads, rddInterval, window, sliding) = args

    val windowSize = window.toInt
    val slidingSize = sliding.toInt

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

//    splittedRdd map(lArr => (lArr.length, 1)) reduceByKeyAndWindow (sumInt(x => x), Seconds(30), Seconds(10)) print()
    //splittedRdd.filter(lArr => lArr.length < 15).map(lArr => lArr.mkString("|")) print(5)
    //splittedRdd.filter(lArr => lArr.length > 15).map(lArr => lArr.mkString("|")) print(5)

    //val decodedEvents = KafkaUtils.createDirectStream[String, GeminiRaw, StringDecoder, GeminiRawDecoder](ssc, kafkaParams, topics).map(_._2)

    val regularEvent = strEventStream map (l => l.split('\t')) filter (lArr => lArr.length == 18)
    val regularRawEvent = regularEvent map (lArr => new GeminiRaw(lArr))
    val regularParsedEvents = regularRawEvent map (gr => new GeminiParsed(gr))

//    val logLineCountByEdge = regularParsedEvents.map(l => (l.sDns.get, 1)) reduceByKeyAndWindow (sumInt(x => x), Seconds(rddInterval.toInt), Seconds(rddInterval.toInt))
//    logLineCountByEdge.foreachRDD((rdd: RDD[(String, Int)], time: Time) => {
//      println("\n-------------------")
//      println("Time: " + time)
//      println("-------------------")
//      println("Received " + rdd.count() + " events\n")
//      rdd.foreach(l => {
//        println("Edge node name " + l._1 + " received  " + l._2 + " events.")
//      })
//      val total = rdd.reduce((p1, p2) => ("Total", p1._2 + p2._2))
//      println("Total count " + total + " events.")
//    })

    regularParsedEvents map (l => {
      if (l.sDns.get.contains("de-gen")) ("Edge", 1L)
      else ("Mid-Tier", 1L)
    }) reduceByKeyAndWindow (sumLong(x => x), Seconds(windowSize), Seconds(slidingSize)) print()

//    regularParsedEvents map (l => (l.sDns.getOrElse("Unknown"), 1L)) reduceByKeyAndWindow (sumLong(x => x), Seconds(windowSize), Seconds(slidingSize)) foreachRDD((rdd: RDD[(String, Long)], time: Time) => {
//      println("\n-------------------")
//      println("Time: " + time)
//      println("-------------------")
//      rdd.foreach(l => {
//        println("Edge node name " + l._1 + " received  " + l._2 + " events.")
//      })
//    })

    regularParsedEvents map (l => {

      if (l.downloadTime.isDefined && l.downloadTime.get > 1) {

        for (
          i <- List.range(0, l.downloadTime.get);
          t = l.utc.get - i;
          c = l.scBytes.getOrElse(0L) / l.downloadTime.get
        ) yield (t, c)
      } else {
        List((l.utc.get, l.scBytes.getOrElse(0L)))
      }
    }) flatMap( x => x) reduceByKeyAndWindow(sumLong(x => x), Seconds(windowSize), Seconds(slidingSize)) transform( rdd => rdd.sortBy(x => x._2, false)) print()

    regularParsedEvents map (l => {
      if (l.sDns.get.contains("de-gen")) ("Edge bytes", l.scBytes.getOrElse(0L))
      else ("Mid-Tier bytes", l.scBytes.getOrElse(0L))
    }) reduceByKeyAndWindow (sumLong(x => x), Seconds(windowSize), Seconds(slidingSize)) print()

//    regularParsedEvents map (l => (l.sDns.getOrElse("Unknown"), l.scBytes.getOrElse(0L))) reduceByKeyAndWindow (sumLong(x => x), Seconds(windowSize), Seconds(slidingSize)) foreachRDD((rdd: RDD[(String, Long)], time: Time) => {
//      println("\n-------------------")
//      println("Time: " + time)
//      println("-------------------")
//      rdd.foreach(l => {
//        println("Edge node name " + l._1 + " received  " + l._2 + " bytes.")
//      })
//    })

//    regularParsedEvents map (l => (l.chunkType.getOrElse("Other"), 1)) reduceByKeyAndWindow (sumInt(x => x), Seconds(30), Seconds(10)) print()
//    regularParsedEvents map (l => (l.streamType.getOrElse("Other"), 1)) reduceByKeyAndWindow (sumInt(x => x), Seconds(30), Seconds(10)) print()
//    regularParsedEvents map (l => (l.eventType.get, 1)) reduceByKeyAndWindow (sumInt(x => x), Seconds(30), Seconds(10)) print()
//    regularParsedEvents map (l => (l.httpStatusCode.get, 1)) reduceByKeyAndWindow (sumInt(x => x), Seconds(30), Seconds(10)) print()
//    regularParsedEvents map (l => (l.cacheDeliveryType.getOrElse("Unknown"), 1)) reduceByKeyAndWindow (sumInt(x => x), Seconds(30), Seconds(10)) print()
//    regularParsedEvents map (l => (l.cacheDeliveryType.getOrElse("Unknown"), l.contentSize.getOrElse(0L))) reduceByKeyAndWindow (sumLong(x => x), Seconds(30), Seconds(10)) print()
//    regularParsedEvents map (l => (l.mappedUserAgent.get, 1)) reduceByKeyAndWindow (sumInt(x => x), Seconds(30), Seconds(10)) print()

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
