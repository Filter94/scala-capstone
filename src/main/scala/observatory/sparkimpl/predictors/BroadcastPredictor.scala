package observatory.sparkimpl.predictors

import observatory.common.{InverseWeighting, InverseWeightingConfiguration}
import org.apache.spark.sql.Dataset
import observatory.SparkContextKeeper.spark
import spark.implicits._
import observatory.{Location, TempByLocation, Temperature}


object BroadcastPredictor {
  def apply(temperatures: Dataset[TempByLocation], predictorConfiguration: InverseWeightingConfiguration) =
    new BroadcastPredictor(predictorConfiguration, temperatures: Dataset[TempByLocation])
}

/**
  * Broadcast implementation of the predictor. Fastest version so far. Will fail if dataset doesn't fit into a single
  * worker's memory.
  */
class BroadcastPredictor(private val predictorConfiguration: InverseWeightingConfiguration,
                         private val temperatures: Dataset[TempByLocation]) extends SparkPredictor {
  def predictTemperatures(locations: Dataset[Location]): Dataset[Temperature] = {
    val temps = spark.sparkContext.broadcast(temperatures.collect())
    val res = for {
      location <- locations
    } yield {
      val comp = temps.value.map {
        tempByLocation: TempByLocation =>
          val d = InverseWeighting.sphereDistance(location, tempByLocation.location) max predictorConfiguration.epsilon
          val wi = InverseWeighting.w(d, predictorConfiguration.p)
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
