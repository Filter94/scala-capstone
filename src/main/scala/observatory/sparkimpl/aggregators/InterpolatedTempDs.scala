package observatory.sparkimpl.aggregators

import observatory.common.InverseWeighting.{Distance, sphereDistance, w}
import observatory.{Location, Temperature}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}

import scala.math.max

class InterpolatedTempDs(val P: Double, val epsilon: Double)
  extends Aggregator[(Location, Location, Temperature), (Temperature, Temperature), Temperature] {
  override def zero: (Temperature, Temperature) = (0.0, 0.0)

  override def merge(b: (Temperature, Temperature), a: (Distance, Temperature)): (Distance, Temperature) =
    (a._1 + b._1, a._2 + b._2)

  override def reduce(b1: (Temperature, Temperature), b2: (Location, Location, Temperature)): (Temperature, Temperature) =
    (b1, b2) match {
      case ((nomAcc, denomAcc), (xi, location, ui)) =>
        val d = max(sphereDistance(location, xi), epsilon)
        val wi = w(d, P)
        (nomAcc + wi * ui, denomAcc + wi)
    }

  override def finish(reduction: (Temperature, Temperature)): Temperature = reduction._1 / reduction._2

  def bufferEncoder: Encoder[(Distance, Temperature)] =
    Encoders.tuple(Encoders.scalaDouble, Encoders.scalaDouble)

  def outputEncoder: Encoder[Temperature] = Encoders.scalaDouble
}
