package spark

import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.{AvroInputFormat, AvroWrapper}
import org.apache.hadoop.io.NullWritable
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

import org.twc.eventgateway2.avro_refactored._

/**
 * Created by jiarui.yan on 5/29/15.
 */

case class EGW(timestamp_received: Long, eventType: String, customerGUIDHash: String, userIPHash: String)



object SparkApp1 {



  def main (args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("My Spark App Test")
      .set("spark.executor.memory", "1g")
      .set("spark.rdd.compress", "true")
      .set("spark.storage.memoryFraction", "1")

    val sc = new SparkContext(conf)


    val rdd = sc.hadoopFile[AvroWrapper[enriched_event], NullWritable, AvroInputFormat[enriched_event]]("")
    val egwRdd = rdd.map(l => {
      val event = l._1.datum()
      EGW(
        event.getTimestampReceived,
        String.valueOf(event.getCommandData.getEventType),
        String.valueOf(event.getSession.getCustomerGuidMd5SaltHashed),
        String.valueOf(event.getSession.getUserIpAddressMd5SaltHashed)
      )
    })

    val stats = rdd.map(l =>
    {
      val event = l._1.datum()
      if (
        event.getSession == null ||
        event.getCommandData == null ||
        event.getSession.getCustomerGuidMd5SaltHashed == null ||
        event.getSession.getUserIpAddressMd5SaltHashed == null
      )
        ("Invalid", 1)
      else
        ("Valid", 1)

    }).reduceByKey((x,y) => x+y).reduce((p1, p2) => ("", p1._2 + p2._2))//.take(10).foreach(println)


    val count = egwRdd.map(e => (e.customerGUIDHash, 1)).groupByKey()
    count.take(50).foreach(println)

    val data = sc.parallelize(1 to 100000).collect().filter(x => x < 50)

    data.foreach(println)

    val changeTxt = sc.textFile("/Users/jiarui.yan/Works/tools/spark-1.3.1/CHANGES.txt", 2)

    val wordCount = changeTxt.flatMap(l => l.split(" ")).map(w => (w, 1)).reduceByKey((x,y) => x + y)

    println("Reading change log file and thihs file contains followings words and counts:")

    wordCount.collect().foreach(println)

    val letterCount = changeTxt.flatMap(l => l.toCharArray).map(l => (l, 1)).reduceByKey((x,y) => x + y)

    println("Reading change log file and thihs file contains followings letters and counts:")

    letterCount.collect().foreach(println)

  }

}
