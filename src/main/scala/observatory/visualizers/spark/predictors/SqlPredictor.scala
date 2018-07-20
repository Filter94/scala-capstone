package observatory.visualizers.spark.predictors

import observatory.SparkContextKeeper.spark.implicits._
import observatory.visualizers.spark.aggregators.InterpolatedTempSql
import observatory.{Location, TempByLocation, Temperature}
import org.apache.spark.sql.Dataset

object SqlPredictor {
  def apply(epsilon: Double, p: Double): SqlPredictor = new SqlPredictor(epsilon, p)
}

/**
  * Sql style predictor implementation. A little bit faster than a Ds implementation.
  * @param epsilon - value which will replace 0
  * @param p - interpolation parameter
  */
class SqlPredictor(private val epsilon: Double, private val p: Double) extends SparkPredictor with Serializable {
  private val it = new InterpolatedTempSql(p, epsilon)
  def predictTemperatures(temperatures: Dataset[TempByLocation],
                          locations: Dataset[Location]): Dataset[Temperature] = {
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
