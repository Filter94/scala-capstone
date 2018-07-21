package observatory.common

import observatory.parimpl.ParPredictor
import observatory.{Location, Temperature}

import scala.collection.parallel.ParIterable

abstract class InverseWeighingPredictor extends ParPredictor {
  /**
    * Predicts temperature for each location by given temperatures
    * @param locations locations to predict
    * @return
    */
  def predictTemperatures(locations: ParIterable[Location]): ParIterable[Temperature]

  /**
    * @param location     Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(location: Location): Temperature
}
