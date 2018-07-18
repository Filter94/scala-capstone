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
  def apply(width: Int = 360, height: Int = 180, transparency: Int = 255,
            locationsGenerator: LocationsGenerator = NaiveGenerator(),
            p: Double = 3, epsilon: Double = 1E-5): VisualizerConfiguration =
    new VisualizerConfiguration(width, height, transparency, locationsGenerator, p, epsilon)
}

class VisualizerConfiguration(val width: Int, val height: Int,
                              val transparency: Int,
                              val locationsGenerator: LocationsGenerator,
                              val p: Double,
                              val epsilon: Double) extends Serializable
