package observatory.visualizers.par

import observatory.common.generators.TileLocationGenerator
import observatory.common.interpolators.LinearInterpolator
import observatory.common.{ImageConfiguration, InverseWeightingConfiguration, Visualizer}
import observatory.parimpl.{IterableParLocationsGenerator, IterablePixelsComputer, ParInverseWeighingPredictor}
import observatory.{Color, Location, Temperature, Tile}

object TileVisualizer {
  def apply(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)], tile: Tile): TileVisualizer =
    new TileVisualizer(ImageConfiguration.Builder().build, tile, 127,
      InverseWeightingConfiguration.Builder().build, temperatures, colors)

  def apply(imageConfiguration: ImageConfiguration, tile: Tile, transparency: Int,
            predictorConfiguration: InverseWeightingConfiguration,
            temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]
           ): TileVisualizer = new TileVisualizer(imageConfiguration, tile, transparency,
    predictorConfiguration, temperatures, colors)
}

final class TileVisualizer(override val imageConfiguration: ImageConfiguration, private val tile: Tile, private val transparency: Int,
                           private val predictorConfiguration: InverseWeightingConfiguration,
                           private val temperatures: Iterable[(Location, Temperature)], private val colors: Iterable[(Temperature, Color)])
  extends Visualizer(imageConfiguration) {
  val pixelsComputer = IterablePixelsComputer(
    LinearInterpolator(colors),
    IterableParLocationsGenerator(TileLocationGenerator(imageConfiguration, tile), imageConfiguration),
    ParInverseWeighingPredictor(temperatures, predictorConfiguration),
    imageConfiguration, transparency)
}
