package observatory.visualizers.par

import observatory.common.generators.NaiveGenerator
import observatory.common.interpolators.LinearInterpolator
import observatory.common.{ImageConfiguration, InverseWeightingConfiguration, Visualizer}
import observatory.parimpl.{IterableParLocationsGenerator, IterablePixelsComputer, ParInverseWeighingPredictor}
import observatory.{Color, Location, Temperature}

object DefaultVisualizer {
  def apply(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): DefaultVisualizer =
    new DefaultVisualizer(ImageConfiguration.Builder().build, 255,
      InverseWeightingConfiguration.Builder().build, temperatures, colors)

  def apply(imageConfiguration: ImageConfiguration, transparency: Int,
            predictorConfiguration: InverseWeightingConfiguration,
            temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]
           ): DefaultVisualizer = new DefaultVisualizer(
    imageConfiguration, transparency, predictorConfiguration, temperatures, colors)
}

final class DefaultVisualizer(override val imageConfiguration: ImageConfiguration, transparency: Int,
                              predictorConfiguration: InverseWeightingConfiguration,
                              temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)])
  extends Visualizer(imageConfiguration) {
  val pixelsComputer = IterablePixelsComputer(
    LinearInterpolator(colors),
    IterableParLocationsGenerator(NaiveGenerator(), imageConfiguration),
    ParInverseWeighingPredictor(temperatures, predictorConfiguration),
    imageConfiguration, transparency)
}
