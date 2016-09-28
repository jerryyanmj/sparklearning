package workflow

import java.text.SimpleDateFormat
import java.util.{TimeZone, Date}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

case class StringFileLoader(path: String) extends CDNMonad[RDD[String]] {
  override def run(sc: SparkContext): RDD[String] = sc.textFile(path)
}

case class GeminiRawEntryDecoder(stringEntryRDD: RDD[String]) extends CDNMonad[RDD[GeminiRawEntry]] {
  override def run(sc: SparkContext): RDD[GeminiRawEntry] = {


    stringEntryRDD.map(stringEntry => {

      def extractDateTime(str: String): Date = {

        val formatter = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z")
        formatter.setTimeZone(TimeZone.getTimeZone("GMT"))
        formatter.parse(str.substring(str.indexOf('[') + 1).substring(0, str.indexOf(']')-1))
      }

      val entryArr = stringEntry.split('\t')

      GeminiRawEntry(entryArr(2), extractDateTime(entryArr(3)), entryArr(10))
    })
  }
}

case class UserAgentAggregator(rawRDD: RDD[GeminiRawEntry]) extends CDNMonad[RDD[(String, Int)]] {
  override def run(sc: SparkContext): RDD[(String, Int)] = {
    rawRDD.map(r => (r.userAgentStr, 1)).reduceByKey((x,y) => x+y)
  }
}

/**
/Users/jiarui.yan/tools/spark-1.5.2-bin-hadoop2.6/bin/spark-submit \
--class "workflow.TestFlow" \
--master local[2] \
target/scala-2.10/sparklearning-assembly-1.0.jar \
/Users/jiarui.yan/tmp/spark/cdn-data/milnhixd-de-gen0002-2016-09-21-15-45
*/
object TestFlow {
  def main (args: Array[String]): Unit = {

    //val Array(filePath) = args
    val filePath = "/Users/jiarui.yan/tmp/spark/cdn-data/milnhixd-de-gen0002-2016-09-21-15-45"
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Simple Application")

    val sc = new SparkContext(conf)

    val logData = sc.textFile(filePath, 2)
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))

    val f = for {
      strRDD <- StringFileLoader(filePath)
      gRDD <- GeminiRawEntryDecoder(strRDD)
      aRDD <- UserAgentAggregator(gRDD)
    } yield {
      println("Total user agent count is: %s".format(aRDD.count()))
      aRDD.take(100).foreach(println)

      gRDD
    }


    f.run(sc)

  }
}

