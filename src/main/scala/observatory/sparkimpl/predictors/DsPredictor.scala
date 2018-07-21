package observatory.sparkimpl.predictors

import observatory.SparkContextKeeper.spark.implicits._
import observatory.common.InverseWeightingConfiguration
import observatory.sparkimpl.aggregators.InterpolatedTempDs
import observatory.{Location, TempByLocation, Temperature}
import org.apache.spark.sql.Dataset

object DsPredictor {
  def apply(predictorConfiguration: InverseWeightingConfiguration, temperatures: Dataset[TempByLocation]): DsPredictor =
    new DsPredictor(predictorConfiguration, temperatures)
}

/**
  * Type safe predictor implementation
  */
class DsPredictor(private val predictorConfiguration: InverseWeightingConfiguration,
                  private val temperatures: Dataset[TempByLocation])
  extends SparkPredictor {
  private val interpolatedTemp = new InterpolatedTempDs(predictorConfiguration.p, predictorConfiguration.epsilon).toColumn

  def predictTemperatures(locations: Dataset[Location]): Dataset[Temperature] = {
    temperatures
      .crossJoin(locations)
      .map { row =>
        (Location(row.getAs("lat"),
          row.getAs("lon")),
          Location.fromRow(row, "location"),
          row.getAs[Temperature]("temperature"))
      }
      .groupByKey(_._1)
      .agg(interpolatedTemp.name("temperature"))
      .toDF("location", "temperature")
      .orderBy($"location.lat".desc, $"location.lon")
      .select($"temperature".as[Temperature])
  }
}
