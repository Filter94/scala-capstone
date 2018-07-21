package observatory.parimpl

import observatory.Location

import scala.collection.parallel.ParIterable

trait ParLocationsGenerator {
  def generateLocations(): ParIterable[Location]
}
