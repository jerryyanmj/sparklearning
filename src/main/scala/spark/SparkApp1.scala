package spark

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._

/**
 * Created by jiarui.yan on 5/29/15.
 */
object SparkApp1 {

  def main (args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("My Spark App Test")
      .set("spark.executor.memory", "1g")
      .set("spark.rdd.compress", "true")
      .set("spark.storage.memoryFraction", "1")

    val sc = new SparkContext(conf)

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
