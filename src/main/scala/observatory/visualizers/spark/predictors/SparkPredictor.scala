package observatory.visualizers.spark.predictors

import observatory.{Location, TempByLocation, Temperature}
import org.apache.spark.sql.Dataset

trait SparkPredictor {
  /**
    * Predicts temperature for each location by given temperatures
    * @param temperatures known temperatures
    * @param locations locations to predict
    * @return
    */
  def predictTemperatures(temperatures: Dataset[TempByLocation],
                          locations: Dataset[Location]): Dataset[Temperature]
}
