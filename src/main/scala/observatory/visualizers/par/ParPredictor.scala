package observatory.visualizers.par

import observatory.{Location, Temperature}

import scala.collection.parallel.ParIterable

trait ParPredictor {
  def predictTemperatures(temperatures: Iterable[(Location, Temperature)],
                          locations: ParIterable[Location]): ParIterable[Temperature]
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature
}
