package workflow

import org.apache.spark.SparkContext

/**
  * Created by jiarui.yan on 2016-09-27.
  */
trait CDNMonad[A] { previous =>
  def run(sc: SparkContext) : A

  def flatMap[B] (f: A => CDNMonad[B]): CDNMonad[B] =
    new CDNMonad[B] {
      override def run(sc: SparkContext): B = {
        val previousResult: A = previous.run(sc)
        val nextMonad: CDNMonad[B] = f(previousResult)
        nextMonad.run(sc)
      }
    }

  def map[B] (f: A => B) : CDNMonad[B] =
    new CDNMonad[B] {
      override def run(sc: SparkContext): B = f(previous.run(sc))
    }
}
