package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._

/**
 * Created by jiarui.yan on 5/29/15.
 */
object SSApp1 {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[2]").setAppName("Spark Streaming Test")
    val ssc = new StreamingContext(conf, Seconds(10))

    val lines = ssc.socketTextStream("localhost", 9999)

    val words = lines.flatMap(l => l.split(" ")).map(w => (w,1)).reduceByKey( (x, y) => x + y)

    words.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
