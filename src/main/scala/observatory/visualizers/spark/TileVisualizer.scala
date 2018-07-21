package observatory.visualizers.spark

import observatory.common.generators.TileLocationGenerator
import observatory.common.interpolators.LinearInterpolator
import observatory.common.{ImageConfiguration, InverseWeightingConfiguration, Visualizer}
import observatory.sparkimpl.predictors.BroadcastPredictor
import observatory.sparkimpl.{SparkSimpleLocationsGenerator, SparkSimplePixelsComputer}
import observatory._
import org.apache.spark.sql.Dataset

object TileVisualizer {
  def apply(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)], tile: Tile): TileVisualizer =
    new TileVisualizer(ImageConfiguration.Builder().build, tile, 255,
      InverseWeightingConfiguration.Builder().build, Helper.toDs(temperatures), colors)

  def apply(imageConfiguration: ImageConfiguration, tile: Tile, transparency: Int, predictorConfiguration: InverseWeightingConfiguration,
            temperatures: Dataset[TempByLocation], colors: Iterable[(Temperature, Color)]): TileVisualizer =
    new TileVisualizer(imageConfiguration, tile, transparency, predictorConfiguration, temperatures, colors)
}

final class TileVisualizer(override val imageConfiguration: ImageConfiguration, private val tile: Tile, private val transparency: Int,
                           private val predictorConfiguration: InverseWeightingConfiguration,
                           private val temperatures: Dataset[TempByLocation], private val colors: Iterable[(Temperature, Color)])
  extends Visualizer(imageConfiguration) {
  val pixelsComputer = SparkSimplePixelsComputer(
    LinearInterpolator(colors),
    SparkSimpleLocationsGenerator(TileLocationGenerator(imageConfiguration, tile), imageConfiguration),
    BroadcastPredictor(temperatures, predictorConfiguration),
    imageConfiguration, transparency)
}
