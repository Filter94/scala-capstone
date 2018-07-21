package observatory.sparkimpl.predictors

import observatory.{Location, TempByLocation, Temperature}
import org.apache.spark.sql.Dataset

trait SparkPredictor extends Serializable {
  /**
    * Predicts temperature for each location by given temperatures
    * @param locations locations to predict
    * @return
    */
  def predictTemperatures(locations: Dataset[Location]): Dataset[Temperature]
}
