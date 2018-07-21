package observatory.parimpl

import observatory.Location
import observatory.common.ImageConfiguration
import observatory.common.generators.LocationGenerator

import scala.collection.parallel.ParIterable

object IterableParLocationsGenerator {
  def apply(locationGenerator: LocationGenerator, imageConfiguration: ImageConfiguration): IterableParLocationsGenerator =
    new IterableParLocationsGenerator(locationGenerator, imageConfiguration)
}

class IterableParLocationsGenerator(private val locationGenerator: LocationGenerator,
                                    private val imageConfiguration: ImageConfiguration) extends ParLocationsGenerator {
  def generateLocations(): ParIterable[Location] =
    Range(0, imageConfiguration.width * imageConfiguration.height).par
      .map(i => locationGenerator.get(i))
}
