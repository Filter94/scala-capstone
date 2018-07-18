package observatory.helpers.predictors.spark

import observatory.helpers.SparkContextKeeper.spark
import observatory.helpers.SparkContextKeeper.spark.implicits._
import observatory.helpers.VisualizationHelper.{sphereDistance, w}
import observatory.{Location, TempByLocation, Temperature}
import org.apache.spark.sql.Dataset

object BroadcastPredictor {
  def apply(epsilon: Double, p: Double): BroadcastPredictor = new BroadcastPredictor(epsilon, p)
}

/**
  * Broadcast implementation of the predictor. Fastest version so far. Will fail if dataset doesn't fit into a single
  * worker's memory
  * @param epsilon - value which will replace 0
  * @param p - interpolation parameter
  */
class BroadcastPredictor(private val epsilon: Double, private val p: Double) extends Predictor with Serializable {
  def predictTemperatures(temperatures: Dataset[TempByLocation],
                          locations: Dataset[Location]): Dataset[Temperature] = {
    val temps = spark.sparkContext.broadcast(temperatures.collect())
    val res = for {
      location <- locations
    } yield {
      val comp = temps.value.map {
        tempByLocation: TempByLocation =>
          val d = sphereDistance(location, tempByLocation.location) max epsilon
          val wi = w(d, p)
          (wi * tempByLocation.temperature, wi)
      }
      val (nominator: Temperature, denominator: Temperature) = comp.reduce {
        (a: (Temperature, Temperature), b: (Temperature, Temperature)) =>
          (a._1 + b._1, a._2 + b._2)
      }
      nominator / denominator
    }
    res.toDF("temperature").as[Temperature]
  }
}
