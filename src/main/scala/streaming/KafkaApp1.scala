package streaming

import model._
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

    val splittedRdd = strEventStream.map(rdd => rdd.split('\t'))

    splittedRdd map(lArr => (lArr.length, 1)) reduceByKey((x, y) => x + y) print()
    //splittedRdd.filter(lArr => lArr.length < 15).map(lArr => lArr.mkString("|")) print(5)
    //splittedRdd.filter(lArr => lArr.length > 15).map(lArr => lArr.mkString("|")) print(5)

    //val decodedEvents = KafkaUtils.createDirectStream[String, GeminiRaw, StringDecoder, GeminiRawDecoder](ssc, kafkaParams, topics).map(_._2)

    val regularEvent = strEventStream map (l => l.split('\t')) filter (lArr => lArr.length == 18)
    val parsedEvents = regularEvent map (lArr => new GeminiRaw(lArr)) map (gr => new GeminiParsed(gr))

    /**
     * stream.foreachRDD((rdd: RDD[_], time: Time) => {
      val count = rdd.count()
      println("\n-------------------")
      println("Time: " + time)
      println("-------------------")
      println("Received " + count + " events\n")
      totalCount += count
    })
     */



    parsedEvents.map(l => (l.sDns.get, 1)) reduceByKeyAndWindow (sumInt(x => x), Seconds(30), Seconds(10)) print()
    parsedEvents map (l => (l.eventType.get, 1)) reduceByKeyAndWindow (sumInt(x => x), Seconds(30), Seconds(10)) print()
    parsedEvents map (l => (l.cacheDeliveryType.getOrElse("Unknown"), 1)) reduceByKeyAndWindow (sumInt(x => x), Seconds(30), Seconds(10)) print()
    parsedEvents map (l => (l.httpStatusCode.get, 1)) reduceByKeyAndWindow (sumInt(x => x), Seconds(30), Seconds(10)) print()
    parsedEvents map (l => (l.cacheDeliveryType.getOrElse("Unknown"), l.contentSize.getOrElse(0L))) reduceByKeyAndWindow (sumLong(x => x), Seconds(30), Seconds(10)) print()

    val midTierEvent = strEventStream map (l => l.split('\t')) filter (lArr => lArr.length == 10)
    val parsedMidTierEvents = midTierEvent map (lArr => new GeminiRawMiddleTier(lArr)) map (l => new GeminiParsedMiddleTier(l))

    parsedMidTierEvents.filter(l => l.csMethod.contains("CNN") && l.csMethod.contains(".ts")) map (l => (l.csMethod, 1)) reduceByKeyAndWindow (sumInt(x => x), Seconds(30), Seconds(10)) print()

    ssc.start()
    ssc.awaitTermination()
  }

}
