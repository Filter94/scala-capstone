package observatory.sparkimpl

import org.apache.spark.sql.Dataset

import observatory.Location
import observatory.SparkContextKeeper.spark
import observatory.SparkContextKeeper.spark.implicits._
import observatory.common.ImageConfiguration
import observatory.common.generators.LocationGenerator

object SparkSimpleLocationsGenerator {
  def apply(locationGenerator: LocationGenerator, imageConfiguration: ImageConfiguration): SparkLocationsGenerator =
    new SparkSimpleLocationsGenerator(locationGenerator, imageConfiguration)
}

class SparkSimpleLocationsGenerator(val locationGenerator: LocationGenerator,
                                    val imageConfiguration: ImageConfiguration) extends SparkLocationsGenerator {
  def generateLocations(): Dataset[Location] = {
    spark.range(imageConfiguration.width * imageConfiguration.height).map(i => locationGenerator.get(i.toInt))
  }
}
