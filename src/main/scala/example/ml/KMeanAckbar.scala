package example.ml


import example.ml.KMeansTest.{anomalyDetectorBuilder, normalizationConstructor}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import play.api.libs.json.{JsObject, JsValue, Json}

import scala.collection.mutable.Buffer

object KMeanAckbar {

//  def fieldParser[String](json: JsObject, field: String, default: String): String = {
//    (json \ field).asOpt[String] match {
//      case Some(value) => value
//      case None => default
//    }
//  }

  def trapParser(trapStr: String) = {
    val trapJson = Json.parse(trapStr)
    (trapJson \ "message").asOpt[String] match {
      case Some(msg) => msg.substring(msg.indexOf('{'))
      case None => StringUtils.EMPTY
    }
  }

  def categorizationAndLabelConstructor(rawData: RDD[(String, JsObject)]) : ((String, JsObject) => (String, Vector)) = {
    val markets = rawData.map(json => {

      (json._2 \ "market").asOpt[String] match {
        case Some(str) => str
        case None => "Unknown"
      }

    }).distinct().collect().zipWithIndex.toMap
    val deviceNames = rawData.map(json => {

      (json._2 \ "device_name").asOpt[String] match {
        case Some(str) => str
        case None => "Unknown"
      }

      //fieldParser[String](json._2, "device_name", StringUtils.EMPTY)
    }).distinct().collect().zipWithIndex.toMap
    val deviceTypes = rawData.map(json => {
      //fieldParser[String](json._2, "device_type", StringUtils.EMPTY)
      (json._2 \ "device_type").asOpt[String] match {
        case Some(str) => str
        case None => "Unknown"
      }
    }).distinct().collect().zipWithIndex.toMap

    (trapStr: String, trapJson: JsObject) => {

      val vector = Buffer[Double]()

      val label = trapStr
      val market = (trapJson \ "market").asOpt[String] match {
        case Some(str) => str
        case None => "Unknown"
      }
      val deviceName = (trapJson \ "device_name").asOpt[String] match {
        case Some(str) => str
        case None => "Unknown"
      }
      val deviceType = (trapJson \ "device_type").asOpt[String] match {
        case Some(str) => str
        case None => "Unknown"
      }


      val vectorizedMarket = new Array[Double](markets.size)
      vectorizedMarket(markets(market)) = 1.0

      val vectorizedDeviceName = new Array[Double](deviceNames.size)
      vectorizedDeviceName(deviceNames(deviceName)) = 1.0

      val vectorizedDeviceType = new Array[Double](deviceTypes.size)
      vectorizedDeviceType(deviceTypes(deviceType)) = 1.0

      vector.insertAll(0, vectorizedMarket)
      vector.insertAll(0, vectorizedDeviceName)
      vector.insertAll(0, vectorizedDeviceType)

      (label, Vectors.dense(vector.toArray))

    }
  }

  def main(args: Array[String]): Unit = {

    val dataSrc = args(0)
    val sc = new SparkContext(new SparkConf())
    val rawData = sc.textFile(dataSrc)
    val enrichedTraps = rawData.map(t => trapParser(t)).map(strTrap => {
      val trapJson = Json.parse(strTrap)
      val newTrap = if (isEncoder(strTrap)) trapJson.as[JsObject] + ("device_type" -> Json.toJson("encoder"))
      else if (isIRD(strTrap)) trapJson.as[JsObject] + ("device_type" -> Json.toJson("ird"))
      else if (isProbe(strTrap)) trapJson.as[JsObject] + ("device_type" -> Json.toJson("probe"))
      else if (isDCM(strTrap)) trapJson.as[JsObject] + ("device_type" -> Json.toJson("dcm"))
      else trapJson.as[JsObject] + ("device_type" -> Json.toJson("unknown"))
      (strTrap, newTrap)
    })

    val categorizingFunc = categorizationAndLabelConstructor(enrichedTraps)

    val originalAndData = enrichedTraps.map(t => categorizingFunc(t._1, t._2))

    val data = originalAndData.values

    val normalizer = normalizationConstructor(data)

    val anomalyDetector = anomalyDetectorBuilder(data, normalizer)

    val anomalies = originalAndData.filter {
      case (original, datum) => anomalyDetector(datum)
    }.keys

    anomalies.take(50).foreach(println)

    println(originalAndData.count())
    println(anomalies.count())

    sc.stop()

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

}
