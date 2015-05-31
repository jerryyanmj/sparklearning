package streaming

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by jiarui.yan on 5/29/15.
 */
object KafkaApp1 {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[2]").setAppName("Kafka Integration Test")
    val ssc = new StreamingContext(conf, Seconds(5))

    // Connecting to Kafka queue for Gemini 71.74.186.212:9092
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9002")

    val topics = Set("gemini_v03")

    val events = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics).map(_._2)


    val rawFeildCount = events.map(l => l.split('\t').length).print(50)

    val logLineByEdgeNodes = events.map(l => {

      val fields = l.split('\t')
      val serverName = fields(0).substring(fields(0).indexOf("nginx-access: ") + "nginx-access: ".length)
      (serverName, 1)
    }).reduceByKey((x,y) => x + y)


    logLineByEdgeNodes.print(50)




    //stream.count().print()

    ssc.start()
    ssc.awaitTermination()


  }

}
