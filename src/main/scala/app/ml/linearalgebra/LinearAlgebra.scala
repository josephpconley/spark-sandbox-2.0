package ml.linearalgebra

import org.apache.spark.mllib.linalg.{Vector, Vectors}

object LinearAlgebra extends App {
  val sv1: Vector = Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0))
}
