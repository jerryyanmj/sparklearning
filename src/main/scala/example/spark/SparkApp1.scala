package example.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
 * Created by jiarui.yan on 5/29/15.
 */

case class EGW(timestamp_received: Long, eventType: String, customerGUIDHash: String, userIPHash: String)



object SparkApp1 {

  def main (args: Array[String]): Unit = {

    val filePath = args(0)

    val logFile = filePath // Should be some file on your system

    val sc = new SparkContext(new SparkConf())

    val logData = sc.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    sc.stop()


//    val rdd = sc.hadoopFile[AvroWrapper[enriched_event], NullWritable, AvroInputFormat[enriched_event]]("")
//    val egwRdd = rdd.map(l => {
//      val event = l._1.datum()
//      EGW(
//        event.getTimestampReceived,
//        String.valueOf(event.getCommandData.getEventType),
//        String.valueOf(event.getSession.getCustomerGuidMd5SaltHashed),
//        String.valueOf(event.getSession.getUserIpAddressMd5SaltHashed)
//      )
//    })
//
//    val stats = rdd.map(l =>
//    {
//      val event = l._1.datum()
//      if (
//        event.getSession == null ||
//        event.getCommandData == null ||
//        event.getSession.getCustomerGuidMd5SaltHashed == null ||
//        event.getSession.getUserIpAddressMd5SaltHashed == null
//      )
//        ("Invalid", 1)
//      else
//        ("Valid", 1)
//
//    }).reduceByKey((x,y) => x+y).reduce((p1, p2) => ("", p1._2 + p2._2))//.take(10).foreach(println)
//
//
//    val count = egwRdd.map(e => (e.customerGUIDHash, 1)).groupByKey()
//    count.take(50).foreach(println)
//
//    val data = sc.parallelize(1 to 100000).collect().filter(x => x < 50)
//
//    data.foreach(println)
//
//    val changeTxt = sc.textFile("/Users/jiarui.yan/Works/tools/spark-1.3.1/CHANGES.txt", 2)
//
//    val wordCount = changeTxt.flatMap(l => l.split(" ")).map(w => (w, 1)).reduceByKey((x,y) => x + y)
//
//    println("Reading change log file and thihs file contains followings words and counts:")
//
//    wordCount.collect().foreach(println)
//
//    val letterCount = changeTxt.flatMap(l => l.toCharArray).map(l => (l, 1)).reduceByKey((x,y) => x + y)
//
//    println("Reading change log file and thihs file contains followings letters and counts:")
//
//    letterCount.collect().foreach(println)

  }

}
