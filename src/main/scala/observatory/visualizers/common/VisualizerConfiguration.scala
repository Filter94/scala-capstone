package observatory.visualizers.common

import observatory.visualizers.common.generators.{LocationsGenerator, NaiveGenerator}

object VisualizerConfiguration {
  /**
    * Mutable Builder using prototype pattern.
    */
  class Builder {
    var configuration = VisualizerConfiguration(360, 180, 255, NaiveGenerator(), 3, 1E-5)

    def setWidth(width: Int): Builder = {
      configuration = configuration.copy(width = width)
      this
    }

    def setHeight(height: Int): Builder = {
      configuration = configuration.copy(height = height)
      this
    }

    def setTransparency(transparency: Int): Builder = {
      configuration = configuration.copy(transparency = transparency)
      this
    }

    def setLocationsGenerator(locationsGenerator: LocationsGenerator): Builder = {
      configuration = configuration.copy(locationsGenerator = locationsGenerator)
      this
    }

    def setP(p: Double): Builder = {
      configuration = configuration.copy(p = p)
      this
    }

    def setEpsilon(epsilon: Double): Builder = {
      configuration = configuration.copy(epsilon = epsilon)
      this
    }

    def build: VisualizerConfiguration =
      configuration
  }
}

case class VisualizerConfiguration(width: Int, height: Int, transparency: Int, locationsGenerator: LocationsGenerator,
                                   p: Double, epsilon: Double) extends Serializable
