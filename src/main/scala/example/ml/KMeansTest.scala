package example.ml

import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object KMeansTest {

  def distance(a: Vector, b: Vector) =
    math.sqrt(a.toArray.zip(b.toArray).map(p => p._1 - p._2).map(d => d * d).sum)

  def distanceToCenter(datum: Vector, model: KMeansModel) = {
    val cluster = model.predict(datum)
    val clusterCenter = model.clusterCenters(cluster)
    distance(clusterCenter, datum)
  }

  def clusteringScore(data: RDD[Vector], k: Int) = {
    val kmeans = new KMeans()
    kmeans.setK(k)
    val model = kmeans.run(data)
    data.map(d => distanceToCenter(d, model)).mean()
  }

  def clusteringScoreImprove(data: RDD[Vector], k: Int) = {
    val kmeans = new KMeans()
    kmeans.setK(k)
    kmeans.setRuns(10)
    kmeans.setEpsilon(1.0e-6)
    val model = kmeans.run(data)
    data.map(d => distanceToCenter(d, model)).mean()
  }

  def normalizationConstructor(data: RDD[Vector]): (Vector => Vector) = {
    val dataAsArray = data.map(_.toArray)
    val numOfCols = dataAsArray.first().length
    val n = dataAsArray.count()
    val sums = dataAsArray.reduce((a,b)=>a.zip(b).map(t => t._1 + t._2))
    val sumOfSquares = dataAsArray.fold(new Array[Double](numOfCols))((a,b)=>a.zip(b).map(t => t._1 + t._2 * t._2))

    val stdevs = sumOfSquares zip sums map {case (sumSq, sum) => math.sqrt(n * sumSq - sum * sum) / n}

    val means = sums.map(x => x/n)

    (datum: Vector) => {
      val normalizedArray = (datum.toArray, means, stdevs).zipped.map(
        (value, mean, stdev) =>
          if (stdev <= 0) (value - mean)
          else (value - mean) / stdev
      )

      Vectors.dense(normalizedArray)
    }

  }

  def categorizationAndLabelConstructor(rawData: RDD[String]) : (String => (String, Vector)) = {
    val splittedData = rawData.map(_.split(','))
    val protocols = splittedData.map(arr => arr(1)).distinct().collect().zipWithIndex.toMap
    val services = splittedData.map(arr => arr(2)).distinct().collect().zipWithIndex.toMap
    val tcpStates = splittedData.map(arr => arr(3)).distinct().collect().zipWithIndex.toMap

    (line: String) => {

      val buffer = line.split(',').toBuffer
      val protocol = buffer.remove(1)
      val service = buffer.remove(1)
      val tcpState = buffer.remove(1)
      val label = buffer.remove(buffer.length - 1)
      val vector = buffer.map(_.toDouble)

      val newProtocolRepresentation = new Array[Double](protocols.size)
      newProtocolRepresentation(protocols(protocol)) = 1.0

      val newServiceRepresentation = new Array[Double](services.size)
      newServiceRepresentation(services(service)) = 1.0

      val newTcpStateRepresentation = new Array[Double](tcpStates.size)
      newTcpStateRepresentation(tcpStates(tcpState)) = 1.0

      vector.insertAll(1, newTcpStateRepresentation)
      vector.insertAll(1, newServiceRepresentation)
      vector.insertAll(1, newProtocolRepresentation)

      (label, Vectors.dense(vector.toArray))

    }
  }

  def entropy(counts: Iterable[Int]) = {
    val values = counts.filter(_ > 0)
    val n : Double = values.sum
    values.map( v => {
      val p = v/n
      -p * math.log(p)
    }).sum
  }

  def clusteringScoreWithQualityCheck(labelsAndData: RDD[(String, Vector)], k: Int) = {

    val kmeans = new KMeans()
    kmeans.setK(k)
    kmeans.setRuns(10)
    kmeans.setEpsilon(1.0e-6)

    val model = kmeans.run(labelsAndData.values)

    // Predict cluster for each datum
    val labelsAndClusters = labelsAndData.mapValues(v => model.predict(v))

    // Swap keys / values
    val clustersAndLabels = labelsAndClusters.map(p => p.swap)

    // Extract collections of labels, per cluster
    val labelsInCluster = clustersAndLabels.groupByKey().values

    // Count labels in collections
    val labelCounts = labelsInCluster.map(_.groupBy(l => l).map(_._2.size))

    // Average entropy weighted by cluster size
    val n = labelsAndData.count()

    labelCounts.map(m => m.sum * entropy(m)).sum / n

  }

  def anomalyDetectorBuilder(data: RDD[Vector], normalizer: (Vector => Vector)) : (Vector => Boolean) = {
    val normalizedData = data.map(normalizer)
    normalizedData.cache()

    val kmeans = new KMeans()
    kmeans.setK(150)
    kmeans.setRuns(10)
    kmeans.setEpsilon(1.0e-6)
    val model = kmeans.run(normalizedData)

    normalizedData.unpersist()

    val distances = normalizedData.map(d => distanceToCenter(d, model))
    val threshold = distances.top(100).last

    (datum: Vector) => distanceToCenter(normalizer(datum), model) > threshold
  }


  def main(args: Array[String]): Unit = {
    val dataSrc = args(0)

    val sc = new SparkContext(new SparkConf())

    val rawData = sc.textFile(dataSrc)

    //clustering0(rawData)

    //clustering1(rawData)

    //clustering2(rawData)

    //clustering3(rawData)

    clustering4(rawData)

//    detectAnomalies(rawData)

    sc.stop()

  }

  def clustering0(rawData: RDD[String]) = {
    rawData.map(l => l.split(',').last).countByValue().toSeq.sortBy(_._2).reverse.foreach(println)

    val labelsAndData = rawData.map { line =>
      val buffer = line.split(',').toBuffer
      buffer.remove(1,3)
      val label = buffer.remove(buffer.length - 1)
      val vector = Vectors.dense(buffer.map(e => e.toDouble).toArray)
      (label, vector)
    }

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

  def clustering1(rawData: RDD[String]) = {

    val data = rawData.map( line => {
      val buffer = line.split(',').toBuffer
      buffer.remove(1,3)
      buffer.remove(buffer.length - 1)
      Vectors.dense(buffer.map(d => d.toDouble).toArray)
    }).cache()

    ((5 to 30 by 5) map (k => (k, clusteringScore(data, k))) toList).foreach(println)

    ((30 to 100 by 10) map (k => (k, clusteringScoreImprove(data, k))) toList).foreach(println)

    data.unpersist()

  }

  def clustering2(rawData: RDD[String]) = {
    val data = rawData.map( line => {
      val buffer = line.split(',').toBuffer
      buffer.remove(1,3)
      buffer.remove(buffer.length - 1)
      Vectors.dense(buffer.map(d => d.toDouble).toArray)
    })

    val normalizer = normalizationConstructor(data)

    val normalizedData = data.map(d => normalizer(d)).cache()

    (60 to 120 by 10).par.map(k => (k, clusteringScoreImprove(normalizedData, k))).toList.foreach(println)

    normalizedData.unpersist()
  }

  def clustering3(rawData: RDD[String]) = {
    val categorizer = categorizationAndLabelConstructor(rawData)

    val transformedData = rawData.map(d  => categorizer(d)).values

    val normalizer = normalizationConstructor(transformedData)

    val normalizedData = transformedData.map(d => normalizer(d)).cache()

    (80 to 160 by 10).map(k => (k, clusteringScoreImprove(normalizedData, k))).toList.foreach(println)

    normalizedData.unpersist()
  }

  def clustering4(rawData: RDD[String]) = {
    val categorizer = categorizationAndLabelConstructor(rawData)

    val transformedData = rawData.map(d  => categorizer(d))

    val normalizer = normalizationConstructor(transformedData.values)

    val normalizedData = transformedData.mapValues(d => normalizer(d)).cache()

    (80 to 160 by 10).map( k => (k, clusteringScoreWithQualityCheck(normalizedData, k))).toList.foreach(println)

    normalizedData.unpersist()

  }

  def detectAnomalies(rawData: RDD[String]) = {
    val categorizer = categorizationAndLabelConstructor(rawData)

    val originalAndData = rawData.map(line => (line, categorizer(line)._2))

//    originalAndData.take(5).foreach(println)

    val data = originalAndData.values

    val normalizer = normalizationConstructor(data)

    val anomalyDetector = anomalyDetectorBuilder(data, normalizer)

    val anomalies = originalAndData.filter {
      case (original, datum) => anomalyDetector(datum)
    }.keys

    anomalies.take(10).foreach(println)
  }

}
