package streaming

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
--class "streaming.KafkaApp1" \
--master local[2] \
target/scala-2.10/sparklearning-assembly-1.0.jar \
KafkaIntegrationTest \
opstkcld-vm-kfa0001.conops.timewarnercable.com:2181,opstkcld-vm-kfa0002.conops.timewarnercable.com:2181,opstkcld-vm-kfa0003.conops.timewarnercable.com:2181 \
gemini-consumer \
gemini_v02 \
2 \
10 \
60 \
20
 *
 */


object KafkaApp2 extends helper {

  def main(args: Array[String]) {

    val Array(appName, zkQuorum, group, topics, numThreads, rddInterval, window, sliding) = args

    val windowSize = window.toInt
    val slidingSize = sliding.toInt

    val conf = new SparkConf().setAppName(appName)
    val ssc = new StreamingContext(conf, Seconds(rddInterval.toLong))
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val strEventStream = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)



    val splittedRdd = strEventStream.print(10)

    ssc.start()
    ssc.awaitTermination()
  }

}
