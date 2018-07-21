package observatory.visualizers.par

import observatory._
import observatory.common.generators.TileLocationGenerator
import observatory.common.interpolators.LinearInterpolator
import observatory.common.{ImageConfiguration, InverseWeightingConfiguration, Visualizer}
import observatory.parimpl.{GridPixelsComputer, IterableParLocationsGenerator}

object GridVisualizer {
  def apply(grid: GridLocation => Temperature, colors: Iterable[(Temperature, Color)], tile: Tile): GridVisualizer =
    new GridVisualizer(ImageConfiguration.Builder().build, grid, tile, 127,
      InverseWeightingConfiguration.Builder().build, colors)

  def apply(grid: GridLocation => Temperature, colors: Iterable[(Temperature, Color)], tile: Tile,
            imageConfiguration: ImageConfiguration, transparency: Int, predictorConfiguration: InverseWeightingConfiguration) =
    new GridVisualizer(imageConfiguration, grid, tile, transparency,
    predictorConfiguration, colors)
}

final class GridVisualizer(override val imageConfiguration: ImageConfiguration, grid: GridLocation => Temperature,
                           tile: Tile, transparency: Int, predictorConfiguration: InverseWeightingConfiguration,
                           colors: Iterable[(Temperature, Color)])
  extends Visualizer(imageConfiguration) {
  val pixelsComputer = GridPixelsComputer(grid,
    LinearInterpolator(colors),
    IterableParLocationsGenerator(TileLocationGenerator(imageConfiguration, tile), imageConfiguration),
    imageConfiguration, transparency)
}
