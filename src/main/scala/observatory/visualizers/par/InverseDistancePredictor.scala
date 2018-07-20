package observatory.visualizers.par

import observatory.visualizers.common.InverseWeighting
import observatory.{Location, Temperature}

import scala.collection.parallel.ParIterable

object InverseDistancePredictor {
  def apply(epsilon: Double, p: Double): InverseDistancePredictor = new InverseDistancePredictor(epsilon, p)
}

class InverseDistancePredictor(private val epsilon: Double, private val p: Double) extends ParPredictor {
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
      case ((nomAcc: InverseWeighting.Distance, denomAcc: Temperature), (xi: Location, ui: Temperature)) =>
        val d = InverseWeighting.sphereDistance(location, xi) max epsilon
        val wi = InverseWeighting.w(d, p)
        (nomAcc + wi * ui, denomAcc + wi)
    }, {
      (a: (Temperature, Temperature), b: (Temperature, Temperature)) =>
        (a._1 + b._1, a._2 + b._2)
    })
    nominator / denominator
  }
}
