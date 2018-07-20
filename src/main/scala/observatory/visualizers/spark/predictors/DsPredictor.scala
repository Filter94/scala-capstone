package observatory.visualizers.spark.predictors

import observatory.SparkContextKeeper.spark.implicits._
import observatory.visualizers.spark.aggregators.InterpolatedTempDs
import observatory.{Location, TempByLocation, Temperature}
import org.apache.spark.sql.Dataset

object DsPredictor {
  def apply(epsilon: Double, p: Double): DsPredictor = new DsPredictor(epsilon, p)
}

/**
  * Type safe predictor implementation
  * @param epsilon - value which will replace 0
  * @param p - interpolation parameter
  */
class DsPredictor(private val epsilon: Double, private val p: Double) extends SparkPredictor with Serializable {
  private val interpolatedTemp = new InterpolatedTempDs(p, epsilon).toColumn
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
      .groupByKey(_._1)
      .agg(interpolatedTemp.name("temperature"))
      .toDF("location", "temperature")
      .orderBy($"location.lat".desc, $"location.lon")
      .select($"temperature".as[Temperature])
  }
}
