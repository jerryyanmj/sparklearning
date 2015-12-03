package streaming

import java.text.DecimalFormat
import java.util

import model._
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
--class "streaming.GeminiBandwidth" \
--master local[2] \
target/scala-2.10/sparklearning-assembly-1.0.jar \
KafkaIntegrationTest \
opstkcld-vm-kfa0001.conops.timewarnercable.com:2181,opstkcld-vm-kfa0002.conops.timewarnercable.com:2181,opstkcld-vm-kfa0003.conops.timewarnercable.com:2181 \
gemini-consumer \
gemini_v02 \
2 \
10 \
120 \
20
 *
 */

object GeminiBandwidth extends helper {

  def main(args: Array[String]) {

    val Array(appName, zkQuorum, group, topics, numThreads, rddInterval, window, sliding) = args

    val windowSize = window.toInt
    val slidingSize = sliding.toInt

    val conf = new SparkConf().setAppName(appName)
    val ssc = new StreamingContext(conf, Seconds(rddInterval.toLong))
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val strEventStream = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    val regularEvent = strEventStream map (l => l.split('\t')) filter (lArr => lArr.length == 18)
    val regularRawEvent = regularEvent map (lArr => new GeminiRaw(lArr))
    val regularParsedEvents = regularRawEvent map (gr => new GeminiParsed(gr))

    // Cache delivery type
    regularParsedEvents map (l => (l.cacheDeliveryType.getOrElse("Unknown"), 1L)) reduceByKeyAndWindow (sumLong(x => x), Seconds(windowSize), Seconds(slidingSize)) print()

    val stats = regularParsedEvents map {l => (l.sDns.getOrElse("Unknown"), (1L, l.scBytes.getOrElse(0L), if("MISS".equals(l.cacheDeliveryType.getOrElse("Unknown"))) {l.scBytes.getOrElse(0L)} else {0L}, l.utc.getOrElse(0L), l.utc.getOrElse(0L)))} reduceByKeyAndWindow(sumStats, Seconds(windowSize), Seconds(slidingSize))

    stats.foreachRDD((rdd: RDD[(String, (Long, Long, Long, Long, Long))], time: Time) => {

      println("\n-------------------")
      println("Time: " + time + " " + new java.util.Date(time.milliseconds))
      println("-------------------")

      rdd.sortBy({case (dns, (lc, sc, msc, utcMax, utcMin)) => dns}).foreach{case (dns, (lc, sc, msc, utcMax, utcMin)) => {
        val edgeEfficiency = (sc-msc)*1D/(sc*1D)
        val edgePercentFormatter = new DecimalFormat("##.00%")

        val edgeGbs = sc*8D/(60*1024*1024*1024D)
        val edgeGbsFormatter = new DecimalFormat("###.##")

        println(dns + "\t" + lc + "\t" + sc + "\t" + msc + "\t" + edgePercentFormatter.format(edgeEfficiency) + "\t" + edgeGbsFormatter.format(edgeGbs) + "\t" + utcMin + "\t" + utcMax + "\t" + (utcMax - utcMin))
      }}

      val total = rdd.reduce({case ((dns1: String, (c1: Long, sc1: Long, msc1: Long, utcMax1: Long, utcMin1: Long)), (dns2: String, (c2: Long, sc2: Long, msc2: Long, utcMax2: Long, utcMin2: Long))) => ("Total", (c1+c2, sc1+sc2, msc1+msc2, if(utcMax1>utcMax2) utcMax1 else utcMax2, if (utcMin1<utcMin2) utcMin1 else utcMin2 ))})
      val efficiency = (total._2._2-total._2._3)*1D/(total._2._2*1D)
      val percentFormatter = new DecimalFormat("##.00%")

      val gbs = (total._2._2)*8D/(60*1024*1024*1024D)
      val gbsFormatter = new DecimalFormat("###.##")

      println("\nTotal\t" + total._2._1 + "\t" + total._2._2 + "\t" + total._2._3 + "\t" + percentFormatter.format(efficiency) + "\t" + gbsFormatter.format(gbs) + "\t" + total._2._5 + "\t" + total._2._4 + "\t" + (total._2._4 - total._2._5))

    })

    //regularParsedEvents map()

    val bandwidth = regularParsedEvents map (l => {
      if (l.downloadTime.getOrElse(0) <= 1 ) List((l.utc.getOrElse(0L), l.contentSize.getOrElse(0L).toDouble))
      else {
        (for {
          i <- 0 until Math.ceil(l.downloadTime.getOrElse(0).toDouble).toInt
          utc = l.utc.getOrElse(0L)
          download = l.downloadTime.getOrElse(0)
          if (utc != 0 && download != 0)
          avg = l.contentSize.getOrElse(0L) * 8D / download
          t = utc - i
        } yield (t, avg)).toList
      }
    }) flatMap(x => x.toSeq) reduceByKeyAndWindow((x: Double,y: Double) => {x + y}, Seconds(windowSize), Seconds(slidingSize))


    bandwidth.foreachRDD((rdd: RDD[(Long, Double)], time: Time) => {
      println("\n-------------------")
      println("Time: " + time + " " + new java.util.Date(time.milliseconds))
      println("-------------------")

      rdd.sortBy({case (ts, data) => data}, false).take(10).foreach({case (ts, data) =>

        val bandwidth = data/(1024*1024*1024D)
        val bandwidthFormatter = new DecimalFormat("###.##")

        println("Bandwidth at\t" + ts + "\t" + data + "\t" + bandwidthFormatter.format(bandwidth))

      })

    });



    ssc.start()
    ssc.awaitTermination()
  }

}
