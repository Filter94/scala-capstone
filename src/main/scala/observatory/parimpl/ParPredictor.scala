package observatory.parimpl

import observatory.{Location, Temperature}

import scala.collection.parallel.ParIterable

trait ParPredictor {
  def predictTemperatures(locations: ParIterable[Location]): ParIterable[Temperature]
  def predictTemperature(location: Location): Temperature
}
