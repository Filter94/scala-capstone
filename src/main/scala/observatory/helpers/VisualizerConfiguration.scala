package observatory.helpers

import observatory.helpers.generators.{LocationsGenerator, NaiveGenerator}

object VisualizerConfiguration {
  /**
    * @param width - width of a resulting picture
    * @param height - height of a resulting picture
    * @param transparency - transparency of each pixel
    * @param locationsGenerator
    * @param p - power parameter for inverse distance weighting
    * @param epsilon - zeros in computations will be replaced with this value
    */
  def apply(width: Int, height: Int, transparency: Int, locationsGenerator: LocationsGenerator,
            p: Double, epsilon: Double): VisualizerConfiguration =
    new VisualizerConfiguration(width, height, transparency, locationsGenerator, p, epsilon)

  class Builder {
    protected var width: Int = 360
    protected var height: Int = 180
    protected var transparency: Int = 255
    protected var locationsGenerator: LocationsGenerator = NaiveGenerator()
    protected var p: Double = 3
    protected var epsilon: Double = 1E-5

    def setWidth(newWidth: Int): Builder = {
      width = newWidth
      this
    }

    def setHeight(newHeight: Int): Builder = {
      height = newHeight
      this
    }

    def setTransparency(newTransparency: Int): Builder = {
      transparency = newTransparency
      this
    }

    def setLocationsGenerator(newLocationsGenerator: LocationsGenerator): Builder = {
      locationsGenerator = newLocationsGenerator
      this
    }

    def setP(newP: Double): Builder = {
      p = newP
      this
    }

    def setEpsilon(newEpsilon: Double): Builder = {
      epsilon = newEpsilon
      this
    }

    def build: VisualizerConfiguration =
      VisualizerConfiguration(width, height, transparency, locationsGenerator, p, epsilon)
  }
}

class VisualizerConfiguration(val width: Int, val height: Int,
                              val transparency: Int,
                              val locationsGenerator: LocationsGenerator,
                              val p: Double,
                              val epsilon: Double) extends Serializable
