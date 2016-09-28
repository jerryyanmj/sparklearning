package example.sparkworker

import org.apache.spark.SparkContext

/**
  * Created by jiarui.yan on 2016-04-15.
  */
trait Worker[A] { parent: Worker[A] =>

  def run(sc: SparkContext): A

  def flatMap[B] (f: A => Worker[B]): Worker[B] =
    new Worker[B] {
      def run(sc: SparkContext): B = {
        val parentResult: A = parent.run(sc)
        val nextWorker: Worker[B] = f(parentResult)
        nextWorker.run(sc)
      }
    }

  def map[B] (f: A => B): Worker[B] =
    new Worker[B] {
      def run(sc: SparkContext): B =
        f(parent.run(sc))
    }
}
