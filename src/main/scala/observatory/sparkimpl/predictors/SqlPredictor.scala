package observatory.sparkimpl.predictors

import observatory.SparkContextKeeper.spark.implicits._
import observatory.common.InverseWeightingConfiguration
import observatory.sparkimpl.aggregators.InterpolatedTempSql
import observatory.{Location, TempByLocation, Temperature}
import org.apache.spark.sql.Dataset

object SqlPredictor {
  def apply(predictorConfiguration: InverseWeightingConfiguration, temperatures: Dataset[TempByLocation]): SqlPredictor =
    new SqlPredictor(predictorConfiguration, temperatures)
}

/**
  * Sql style predictor implementation. A little bit faster than a Ds implementation.
  */
class SqlPredictor(private val predictorConfiguration: InverseWeightingConfiguration,
                   private val temperatures: Dataset[TempByLocation])
  extends SparkPredictor {

  private val it = new InterpolatedTempSql(predictorConfiguration.p, predictorConfiguration.epsilon)
  def predictTemperatures(locations: Dataset[Location]): Dataset[Temperature] = {
    temperatures
      .crossJoin(locations)
      .map { row =>
        (Location(row.getAs("lat"),
          row.getAs("lon")),
          Location.fromRow(row, "location"),
          row.getAs[Temperature]("temperature"))
      }
      .groupBy($"_1".as("location"))
      .agg(it($"_1", $"_2", $"_3").as("temperature"))
      .orderBy($"location.lat".desc, $"location.lon")
      .select($"temperature".as[Temperature])
  }
}
