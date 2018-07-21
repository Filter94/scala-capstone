package observatory.parimpl

import observatory.common.{InverseWeighingPredictor, InverseWeighting, InverseWeightingConfiguration}
import observatory.{Location, Temperature}

import scala.collection.parallel.ParIterable

object ParInverseWeighingPredictor {
  def apply(temperatures: Iterable[(Location, Temperature)], predictorConfiguration: InverseWeightingConfiguration): ParInverseWeighingPredictor =
    new ParInverseWeighingPredictor(temperatures, predictorConfiguration)
}

class ParInverseWeighingPredictor(temperatures: Iterable[(Location, Temperature)],
                                  predictorConfiguration: InverseWeightingConfiguration)
  extends InverseWeighingPredictor {
  /**
    * Predicts temperature for each location by given temperatures
    * @param locations locations to predict
    * @return
    */
  override def predictTemperatures(locations: ParIterable[Location]): ParIterable[Temperature] = {
    for {
      location <- locations
    } yield predictTemperature(location)
  }

  /**
    * @param location     Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  override def predictTemperature(location: Location): Temperature = {
    val (nominator, denominator) = temperatures.aggregate((0.0, 0.0))({
      case ((nomAcc: InverseWeighting.Distance, denomAcc: Temperature), (xi: Location, ui: Temperature)) =>
        val d = InverseWeighting.sphereDistance(location, xi) max predictorConfiguration.epsilon
        val wi = InverseWeighting.w(d, predictorConfiguration.p)
        (nomAcc + wi * ui, denomAcc + wi)
    }, {
      (a: (Temperature, Temperature), b: (Temperature, Temperature)) =>
        (a._1 + b._1, a._2 + b._2)
    })
    nominator / denominator
  }
}
