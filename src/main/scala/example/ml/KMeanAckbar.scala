package example.ml


import example.ml.KMeansTest._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import play.api.libs.json.{JsObject, Json}

import scala.collection.mutable

trait ToTSV {
  this: Product =>
  override def toString = productIterator.mkString(",")
}

case class TransportAlarm(deviceName: String, deviceType: String, market: String, error: String) extends ToTSV

object KMeanAckbar {

  def dataClean(rawData: RDD[String]) = {
    val rawTrap = rawData.map(trapParser)

    val dcmRawTrap = rawTrap.filter(trapStr => isDCM(trapStr))

    val encoderRawTrap = rawTrap.filter(trapStr => isEncoder(trapStr))

    val dcmAlarms = dcmRawTrap.map(dcmTrapStr => {

      val jsonTrap = Json.parse(dcmTrapStr)

      val deviceName = (jsonTrap \ "device_name").asOpt[String] match {
        case Some(str) => str
        case None => "Unknown"
      }

      val market = (jsonTrap \ "market").asOpt[String] match {
        case Some(str) => str
        case None => "Unknown"
      }

      val error = (jsonTrap \ "values" \ "text").asOpt[String] match {
        case Some(str) => str
        case None => "Unknown"
      }

      TransportAlarm(deviceName, "dcm", market, error).toString
    })

    val encoderAlarms = encoderRawTrap.map(encoderTrapStr => {

      val jsonTrap = Json.parse(encoderTrapStr)

      val deviceName = (jsonTrap \ "device_name").asOpt[String] match {
        case Some(str) => str
        case None => "Unknown"
      }

      val market = (jsonTrap \ "market").asOpt[String] match {
        case Some(str) => str
        case None => "Unknown"
      }

      val error = (jsonTrap \ "values" \ "alarm").asOpt[String] match {
        case Some(str) => str
        case None => "Unknown"
      }

      TransportAlarm(deviceName, "encoder", market, error).toString
    })

    encoderAlarms.take(10).foreach(println)
    dcmAlarms.take(10).foreach(println)

    encoderAlarms ++ dcmAlarms
  }

  def main(args: Array[String]): Unit = {

    val dataSrc = args(0)
    val sc = new SparkContext(new SparkConf())
    val rawData = sc.textFile(dataSrc)

    val cleanedRawData = dataClean(rawData)

    //trapClustering0(cleanedRawData)

    //trapClustering1(cleanedRawData)

    //trapClustering2(cleanedRawData)

//    checkTrapClustering2(cleanedRawData)

//    trapClustering3(cleanedRawData)

    detectAnomalies(cleanedRawData)

    sc.stop()

  }

  def trapClustering0(rawData: RDD[String]) = {

    val vectorGenerator = vectorGeneratorBuilder(rawData)

    val labelsAndData = rawData.map(vectorGenerator)

    val data = labelsAndData.values.cache()

    val kmeans = new KMeans()
    val model = kmeans.run(data)

    model.clusterCenters.foreach(println)

    val clusterLabelCount = labelsAndData.map { case (label, datum) =>
      val cluster = model.predict(datum)
      (cluster, label)
    }.countByValue

    clusterLabelCount.toSeq.sorted.foreach{
      case ((cluster, label), count) =>
        println(f"$cluster%1s$label%18s$count%8s")
    }

    data.unpersist()
  }

  def trapClustering1(rawData: RDD[String]) = {

    val vectorGenerator = vectorGeneratorBuilder(rawData)

    val labelsAndData = rawData.map(vectorGenerator)

    val data = labelsAndData.values.cache()

    ((5 to 30 by 5) map (k => (k, clusteringScore(data, k))) toList).foreach(println)

    ((30 to 100 by 10) map (k => (k, clusteringScoreImprove(data, k))) toList).foreach(l => {
      val kValue = l._1
      val dist = l._2

      println(f"$kValue%8s$dist%1.14f")
    })

    data.unpersist()

  }


  def trapClustering2(rawData: RDD[String]) = {
    val vectorGenerator = vectorGeneratorBuilder(rawData)

    val labelsAndData = rawData.map(vectorGenerator)

    val data = labelsAndData.values

    val normalizer = normalizationConstructor(data)

    val normalizedData = data.map(d => normalizer(d)).cache()

    (60 to 160 by 10).par.map(k => (k, clusteringScoreImprove(normalizedData, k))).toList.foreach(println)

    normalizedData.unpersist()
  }

  // TODO: convert the count to percentage


  def checkTrapClustering2(rawData: RDD[String]) = {

    val vectorGenerator = vectorGeneratorBuilder(rawData)

    val labelsAndData = rawData.map(vectorGenerator)

    val data = labelsAndData.values

    val normalizer = normalizationConstructor(data)

    val normalizedData = data.map(d => normalizer(d)).cache()



    val kmeans = new KMeans()
    kmeans.setK(160)
    kmeans.setRuns(10)
    kmeans.setEpsilon(1.0e-6)
    val model = kmeans.run(normalizedData)
    data.map(d => distanceToCenter(d, model)).mean()

    model.clusterCenters.foreach(println)

    val clusterLabelCount = labelsAndData.map { case (label, datum) =>
      val cluster = model.predict(datum)
      (cluster, label)
    }.countByValue

    clusterLabelCount.toSeq.sorted.foreach{
      case ((cluster, label), count) =>
        println(f"$cluster%1s$label%18s$count%8s")
    }

    data.unpersist()
  }



  def trapClustering3(rawData: RDD[String]) = {
    val vectorGenerator = vectorGeneratorBuilder(rawData)

    val labelsAndData = rawData.map(vectorGenerator)

    val normalizer = normalizationConstructor(labelsAndData.values)

    val normalizedData = labelsAndData.mapValues(d => normalizer(d)).cache()

    (80 to 160 by 10).map( k => (k, clusteringScoreWithQualityCheck(normalizedData, k))).toList.foreach(println)

    normalizedData.unpersist()

  }

  def anomalyDetectorBuilder(data: RDD[Vector], normalizer: (Vector => Vector)) : (Vector => Boolean) = {
    val normalizedData = data.map(normalizer)
    normalizedData.cache()

    val kmeans = new KMeans()
    kmeans.setK(120)
    kmeans.setRuns(10)
    //kmeans.setEpsilon(1.0e-6)
    val model = kmeans.run(normalizedData)

    normalizedData.unpersist()

    val distances = normalizedData.map(d => distanceToCenter(d, model))
    val threshold = distances.top(160).last

    (datum: Vector) => distanceToCenter(datum, model) > threshold
  }


  def detectAnomalies(rawData: RDD[String]) = {

    val vectorGenerator = vectorGeneratorBuilder(rawData)

    val originalAndData = rawData.map(line => (line, vectorGenerator(line)._2))

    originalAndData.take(5).foreach(println)

    val data = originalAndData.values

    val normalizer = normalizationConstructor(data)

    val anomalyDetector = anomalyDetectorBuilder(data, normalizer)

    val anomalies = originalAndData.filter {
      case (original, datum) => anomalyDetector(datum)
    }.keys

    anomalies.take(10).foreach(println)

    println(originalAndData.count())
    println(anomalies.count())
  }

  /** Predicates for encoders */
  def isEncoder: (String => Boolean) = {
    str =>
      str.contains("AVP2000") ||
        str.contains("AVP4000") || str.contains("C2") ||
        str.contains("Electra 8240") || str.contains("Muse") ||
        str.contains("SPR1100")
  }

  /** Predicates for IRDs */
  def isIRD: (String => Boolean) = {
    str => str.contains("D9854") || str.contains("Proview 7134")
  }

  /** Predicates for probes */
  def isProbe: (String => Boolean) = {
    str => str.contains("Sentry") || str.contains("iVMS")
  }

  /** Predicates for DCMs */
  def isDCM: (String => Boolean) = {
    str =>
      str.contains("DCM9900") ||
        str.contains("DCM9902")
  }


  def trapParser(trapStr: String) = {
    val trapJson = Json.parse(trapStr)
    (trapJson \ "message").asOpt[String] match {
      case Some(msg) => msg.substring(msg.indexOf('{'))
      case None => StringUtils.EMPTY
    }
  }



  def vectorGeneratorBuilder(cleanedRawData: RDD[String]) : (String => (String, Vector)) = {
    val splittedData = cleanedRawData.map(_.split(','))
    val devices = splittedData.map(arr => arr(0)).distinct().collect().zipWithIndex.toMap
    val deviceTypes = splittedData.map(arr => arr(1)).distinct().collect().zipWithIndex.toMap
    val markets = splittedData.map(arr => arr(2)).distinct().collect().zipWithIndex.toMap
    val errors = splittedData.map(arr => arr(3)).distinct().collect().zipWithIndex.toMap

    (trapStr: String) => {

      val cols = trapStr.split(',')


      val vector = mutable.Buffer[Double]()

      val label = cols(0)
      val deviceType = cols(1)
      val market = cols(2)
      val error = cols(3)

      val vectorizedDevice = new Array[Double](devices.size)
      vectorizedDevice(devices(label)) = 1.0


      val vectorizedMarket = new Array[Double](markets.size)
      vectorizedMarket(markets(market)) = 1.0

      val vectorizedDeviceType = new Array[Double](deviceTypes.size)
      vectorizedDeviceType(deviceTypes(deviceType)) = 1.0

      val vectorizedError = new Array[Double](errors.size)
      vectorizedError(errors(error)) = 1.0

      vector.insertAll(0, vectorizedDeviceType)
      vector.insertAll(0, vectorizedMarket)
      //vector.insertAll(0, vectorizedError)
      vector.insertAll(0, vectorizedDevice)

      (label, Vectors.dense(vector.toArray))

    }
  }

}
