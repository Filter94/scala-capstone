package observatory.helpers.predictors

import observatory.{Location, Temperature}
import observatory.helpers.VisualizationHelper

import scala.collection.parallel.ParIterable

object ParPredictor {
  def apply(epsilon: Double, p: Double): ParPredictor = new ParPredictor(epsilon, p)
}

class ParPredictor(private val epsilon: Double, private val p: Double) {
  /**
    * Predicts temperature for each location by given temperatures
    * @param temperatures known temperatures
    * @param locations locations to predict
    * @return
    */
  def predictTemperatures(temperatures: Iterable[(Location, Temperature)],
                          locations: ParIterable[Location]): ParIterable[Temperature] = {
    for {
      location <- locations
    } yield predictTemperature(temperatures, location)
  }

  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location     Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature = {
    val (nominator, denominator) = temperatures.aggregate((0.0, 0.0))({
      case ((nomAcc: VisualizationHelper.Distance, denomAcc: Temperature), (xi: Location, ui: Temperature)) =>
        val d = VisualizationHelper.sphereDistance(location, xi) max epsilon
        val wi = VisualizationHelper.w(d, p)
        (nomAcc + wi * ui, denomAcc + wi)
    }, {
      (a: (Temperature, Temperature), b: (Temperature, Temperature)) =>
        (a._1 + b._1, a._2 + b._2)
    })
    nominator / denominator
  }
}
